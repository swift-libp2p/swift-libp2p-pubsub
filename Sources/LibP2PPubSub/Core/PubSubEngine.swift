//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2026 swift-libp2p project authors
// Licensed under MIT
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of swift-libp2p project authors
//
// SPDX-License-Identifier: MIT
//
//===----------------------------------------------------------------------===//

import LibP2P

/// Errors thrown by the PubSub routers
public enum PubSubError: Error, Equatable, Sendable {

    /// The router isn't running (it hasn't booted yet, or it has shut down)
    case notRunning

    /// Topics must be non-empty strings
    case invalidTopic

    /// Every subscription to a topic must use the same signature policy
    case conflictingSignaturePolicy(topic: String)

    /// Our ``PubSubConfiguration/subscriptionFilter`` rejected subscribing to this topic
    case topicNotAllowed(topic: String)

}

/// The common sub routines that every PubSub router depends on.
///
/// The engine owns all of a router's mutable state and processes events one at a time,
/// - our topic subscriptions (their signature policy, message ID strategy, validators and event sinks)
/// - the seen cache used for duplicate message suppression
/// - the per peer outbound queues
/// - the routing algorithm's own state (``PubSubRouter``)
///
/// ## Streams
/// PubSub uses a pair of unidirectional streams per peer. We read the RPCs a peer sends us on the stream it opened,
/// and write the RPCs we send it on the stream we opened. Our writes are queued (``PubSubConfiguration/outboundQueueSize``)
/// and drained by a writer task, so producing an RPC never waits on the network. The first RPC on each of our streams
/// announces our subscriptions.
///
/// ## Inbound messages
/// Each inbound message must conform to its topic's signature policy, be new (unseen), and pass the topic's validations.
/// Only then is it marked as seen, delivered to our subscribers, and forwarded according to the ``PubSubRouter``.
actor PubSubEngine {
    typealias Instant = ContinuousClock.Instant

    /// The protocols we speak, in order of preference, ex: `[/meshsub/1.0.0, /floodsub/1.0.0]`
    nonisolated let protocolIDs: [String]
    nonisolated let localPeer: PeerID
    nonisolated let configuration: PubSubConfiguration
    nonisolated let logger: Logger

    private(set) var router: any PubSubRouter
    private(set) var isRunning = false

    private var seen: SeenCache
    private var topics: [String: TopicState] = [:]
    private var peers: [PeerID: PeerState] = [:]
    private var heartbeatTask: Task<Void, Never>?
    private var discoveryTask: Task<Void, Never>?
    private var sequenceNumber: UInt64
    private let clock = ContinuousClock()

    /// Connects to peers the router asks for (ex: direct peers, or peers suggested via peer exchange)
    /// The peer's (verified) record, when we have one, tells the dialer where to find it.
    private let dialer: (@Sendable (PeerID, PeerRecord?) async -> Void)?
    
    /// The peers we're currently dialing
    private var pendingDials: Set<PeerID> = []

    init(
        protocolIDs: [String],
        localPeer: PeerID,
        configuration: PubSubConfiguration,
        router: any PubSubRouter,
        logger: Logger,
        dialer: (@Sendable (PeerID, PeerRecord?) async -> Void)? = nil
    ) {
        precondition(!protocolIDs.isEmpty, "A PubSub engine must speak at least one protocol")
        self.protocolIDs = protocolIDs
        self.localPeer = localPeer
        self.configuration = configuration
        self.router = router
        self.logger = logger
        self.dialer = dialer
        self.seen = SeenCache(ttl: configuration.seenTTL)
        /// Like go-libp2p-pubsub, sequence numbers start at the current time (in nanoseconds) and increase monotonically
        self.sequenceNumber = UInt64(max(0, Date().timeIntervalSince1970 * 1_000_000_000))
    }

    // MARK: - Lifecycle

    /// Starts the heartbeat and, if provided, starts discovering peers via `peerEvents`
    ///
    /// - Parameter peerEvents: Our application's `remotePeerProtocolChange` and `identifiedPeer` events
    ///   (`app.events.subscribe(to:)`). Consuming them from a single stream means we handle peers in the order they're
    ///   identified. Identify events carry the peer's signed peer record, which we pass on to routers that want them.
    func start(peerEvents: AsyncStream<EventBus.EventEmitter>? = nil) {
        guard !self.isRunning else { return }
        self.isRunning = true
        let interval = self.configuration.heartbeatInterval
        self.heartbeatTask = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: interval) } catch { return }
                guard let self else { return }
                await self.heartbeat()
            }
        }
        if let peerEvents {
            let protocolIDs = self.protocolIDs
            let wantsSignedPeerRecords = self.router.wantsSignedPeerRecords
            self.discoveryTask = Task { [weak self] in
                for await event in peerEvents {
                    switch event {
                    case .remotePeerProtocolChange(let change):
                        await self?.peerProtocolsChanged(
                            change.peer,
                            protocols: change.protocols.map(\.stringValue),
                            connection: change.connection
                        )
                    case .identifiedPeer(let identified) where wantsSignedPeerRecords:
                        /// Verify the record here, rather than on the actor
                        guard
                            let record = SignedPeerRecord(
                                identify: identified.identity,
                                from: identified.peer,
                                speakingAnyOf: protocolIDs
                            )
                        else { continue }
                        await self?.addSignedPeerRecord(record)
                    default:
                        continue
                    }
                }
            }
        }
    }

    /// Stops the heartbeat, closes our outbound streams and ends every subscription
    func stop() async {
        guard self.isRunning else { return }
        self.isRunning = false

        let heartbeat = self.heartbeatTask
        let discovery = self.discoveryTask
        self.heartbeatTask = nil
        self.discoveryTask = nil
        heartbeat?.cancel()
        discovery?.cancel()
        await heartbeat?.value
        await discovery?.value

        for peer in Array(self.peers.keys) { self.removePeer(peer) }
        for (topic, state) in self.topics {
            for registration in state.registrations.values { registration.finish() }
            _ = self.router.leave(topic, now: self.clock.now)
        }
        self.topics.removeAll()
    }

    // MARK: - Subscriptions

    /// Subscribes to a topic, returning an `AsyncSequence` of its events
    func subscribe(_ config: TopicConfiguration) throws -> PubSubSubscription {
        let id = UUID()
        let topic = config.topic
        let (events, continuation) = AsyncStream.makeStream(
            of: PubSub.SubscriptionEvent.self,
            bufferingPolicy: .bufferingOldest(self.configuration.subscriptionBufferSize)
        )
        try self.register(
            config,
            key: .subscription(id),
            registration: Registration(validator: config.validator, events: continuation)
        )
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeRegistration(topic: topic, key: .subscription(id)) }
        }
        return PubSubSubscription(topic: topic, events: events, onCancel: { continuation.finish() })
    }

    /// Subscribes to a topic on behalf of a swift-libp2p-core `SubscriptionHandler`, replacing any previous handler for the topic
    func subscribe(_ config: TopicConfiguration, handler: LegacySubscriptionHandler) throws {
        try self.register(
            config,
            key: .legacyHandler,
            registration: Registration(validator: config.validator, legacyHandler: handler)
        )
    }

    /// Subscribes to a topic without consuming its events (we still participate in routing the topic's messages)
    func join(_ config: TopicConfiguration) throws {
        try self.register(config, key: .join, registration: Registration(validator: config.validator))
    }

    /// Unsubscribes from a topic entirely, ending all of its subscriptions
    func unsubscribe(from topic: String) {
        guard let state = self.topics[topic] else { return }
        for registration in state.registrations.values { registration.finish() }
        self.leave(topic)
    }

    func subscribedTopics() -> [String] {
        Array(self.topics.keys)
    }

    func peers(subscribedTo topic: String) -> [PeerID] {
        Array(self.router.peers(subscribedTo: topic))
    }

    /// Gives tests read access to the routing algorithm's state
    func inspectRouter<T: Sendable>(_ body: @Sendable (any PubSubRouter) -> T) -> T {
        body(self.router)
    }

    private func register(_ config: TopicConfiguration, key: RegistrationKey, registration: Registration) throws {
        guard !config.topic.isEmpty else { throw PubSubError.invalidTopic }
        guard self.configuration.subscriptionFilter.allows(config.topic) else {
            throw PubSubError.topicNotAllowed(topic: config.topic)
        }
        if let existing = self.topics[config.topic] {
            guard existing.signaturePolicy == config.signaturePolicy else {
                throw PubSubError.conflictingSignaturePolicy(topic: config.topic)
            }
            self.topics[config.topic]?.registrations.updateValue(registration, forKey: key)?.finish()
        } else {
            self.topics[config.topic] = TopicState(
                signaturePolicy: config.signaturePolicy,
                messageID: config.effectiveMessageID,
                registrations: [key: registration]
            )
            self.logger.debug("Subscribed to `\(config.topic)`")
            // notify our router of the join
            var outbox = self.router.join(config.topic, now: self.clock.now)
            self.announce(config.topic, subscribed: true, into: &outbox)
            self.flush(outbox)
        }
    }

    private func removeRegistration(topic: String, key: RegistrationKey) {
        guard self.topics[topic]?.registrations.removeValue(forKey: key) != nil else { return }
        if self.topics[topic]?.registrations.isEmpty == true { self.leave(topic) }
    }

    private func leave(_ topic: String) {
        self.topics.removeValue(forKey: topic)
        self.logger.debug("Unsubscribed from `\(topic)`")
        // notify our router of the unsub
        var outbox = self.router.leave(topic, now: self.clock.now)
        self.announce(topic, subscribed: false, into: &outbox)
        self.flush(outbox)
    }

    /// Tells every peer about a change to our subscriptions
    private func announce(_ topic: String, subscribed: Bool, into outbox: inout Outbox) {
        let rpc = RPC.with {
            $0.subscriptions = [
                .with {
                    $0.topicID = topic
                    $0.subscribe = subscribed
                }
            ]
        }
        for peer in self.peers.keys { outbox.send(rpc, to: peer) }
    }

    // MARK: - Publishing

    /// Publishes `data` to `topic`.
    /// - Note: We don't need to be subscribed to the topic.
    func publish(_ data: Data, to topic: String) throws {
        guard self.isRunning else { throw PubSubError.notRunning }
        guard !topic.isEmpty else { throw PubSubError.invalidTopic }

        let state = self.topics[topic]
        let policy = state?.signaturePolicy ?? self.configuration.defaultSignaturePolicy
        let messageID = state?.messageID ?? .default(for: policy)

        var message = RPC.Message()
        message.data = data
        message.topicIds = [topic]
        if case .strictSign = policy {
            message.from = Data(self.localPeer.id)
            message.seqno = self.nextSequenceNumber()
        }
        message = try MessageSigning.prepare(message, policy: policy, signer: self.localPeer)

        let id = messageID.id(for: message)

        /// Remember our own message so we drop it if it's echoed back to us
        self.seen.insert(id, now: self.clock.now)

        if self.configuration.emitSelf, let state { self.deliver(.data(message), to: state) }

        var outbox = Outbox()
        for peer in self.router.route(message, id: id, topic: topic, from: nil, now: self.clock.now) {
            outbox.send(messages: [message], to: peer)
        }
        self.flush(outbox)
    }

    private func nextSequenceNumber() -> Data {
        self.sequenceNumber &+= 1
        return withUnsafeBytes(of: self.sequenceNumber.bigEndian) { Data($0) }
    }

    // MARK: - Peers & Streams

    /// Called when a peers supported protocols change (ex: once it's been identified).
    ///
    /// If the peer speaks one of our protocols, and we don't have an open stream for it yet, we open a new stream
    /// directed at the most preferred protocol we share in common.
    /// - Note: A peer's streams (rather than connection events) determine when we forget about it, see ``inboundStreamClosed(_:)``
    ///   and ``detachWriter(from:token:)``.
    func peerProtocolsChanged(_ peer: PeerID, protocols: [String], connection: Connection) {
        guard self.isRunning, peer != self.localPeer else { return }
        /// protocolIDs is in preferred order (so first returns our most preferred shared protocol)
        guard let protocolID = self.protocolIDs.first(where: protocols.contains) else { return }
        let state = self.peers[peer] ?? PeerState(bufferSize: self.configuration.outboundQueueSize)
        self.peers[peer] = state
        if state.writer == nil { self.openOutboundStream(protocolID, on: connection) }
    }

    /// Drives a stream negotiated for one of our protocols, for as long as it's open
    nonisolated func run(_ stream: LibP2PStream) async {
        guard let peer = stream.remotePeer else {
            self.logger.warning("Ignoring a `\(stream.protocol)` stream without an authenticated remote peer")
            return
        }
        guard peer != self.localPeer else { return }
        switch stream.direction {
        case .inbound:
            await self.runInbound(stream, from: peer)
        case .outbound:
            await self.runOutbound(stream, to: peer)
        }
    }

    /// Reads the RPCs `peer` sends us, one at a time
    private nonisolated func runInbound(_ stream: LibP2PStream, from peer: PeerID) async {
        guard await self.inboundStreamOpened(peer, protocolID: stream.protocol, connection: stream.connection) else {
            return
        }
        do {
            for try await frame in stream.inbound {
                await self.handle(frame, from: peer)
            }
        } catch {
            self.logger.debug("Inbound stream from \(peer) failed: \(error)")
        }
        await self.inboundStreamClosed(peer)
    }

    /// Writes our queued RPCs to `peer`, until the stream closes or the peer is removed
    private nonisolated func runOutbound(_ stream: LibP2PStream, to peer: PeerID) async {
        let outbound = stream.connection.direction == .outbound
        guard let writer = await self.attachWriter(to: peer, protocolID: stream.protocol, outbound: outbound) else {
            self.logger.debug("Closing a redundant outbound stream to \(peer)")
            return
        }
        let logger = self.logger
        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                do {
                    if let hello = writer.hello { try await stream.write(hello) }
                    for await frame in writer.queue { try await stream.write(frame) }
                } catch {
                    logger.debug("Outbound stream to \(peer) failed: \(error)")
                }
            }
            group.addTask {
                /// Peers never write on the streams we open, so this only returns once the stream closes
                do { for try await _ in stream.inbound {} } catch {}
            }
            /// Whichever finishes first (the queue was finished, or the stream closed) ends the other
            await group.next()
            group.cancelAll()
        }
        await self.detachWriter(from: peer, token: writer.token)
    }

    private func inboundStreamOpened(_ peer: PeerID, protocolID: String, connection: Connection) -> Bool {
        guard self.isRunning else { return false }
        var state = self.peers[peer] ?? PeerState(bufferSize: self.configuration.outboundQueueSize)
        state.inboundStreams += 1
        self.peers[peer] = state
        if state.writer == nil {
            /// Until we open our own stream, assume the peer speaks the protocol it chose for its stream
            self.router.addPeer(peer, protocolID: protocolID, outbound: connection.direction == .outbound)
            /// Make sure we have a mirrored, write side, stream to this peer (for the same protocol)
            /// Ou peerEvent / protocol change event will check before opening another stream for the same protocol
            self.openOutboundStream(protocolID, on: connection)
        }
        return true
    }

    private func inboundStreamClosed(_ peer: PeerID) {
        guard var state = self.peers[peer] else { return }
        state.inboundStreams = max(0, state.inboundStreams - 1)
        if state.inboundStreams == 0 && state.writer == nil {
            self.removePeer(peer)
        } else {
            self.peers[peer] = state
        }
    }

    /// - Parameter outbound: Whether we dialed the connection this stream is on
    private func attachWriter(to peer: PeerID, protocolID: String, outbound: Bool) -> Writer? {
        guard self.isRunning else { return nil }
        var state = self.peers[peer] ?? PeerState(bufferSize: self.configuration.outboundQueueSize)
        guard state.writer == nil else { return nil }
        let token = UUID()
        state.writer = token
        self.peers[peer] = state
        /// The router needs to know what protocol this peer is speaking, so pass it along...
        self.router.addPeer(peer, protocolID: protocolID, outbound: outbound)
        return Writer(token: token, queue: state.queue, hello: self.helloFrame())
    }

    private func detachWriter(from peer: PeerID, token: UUID) {
        guard var state = self.peers[peer], state.writer == token else { return }
        state.writer = nil
        /// A queue can only be consumed once, so start a fresh one for the next outbound stream
        state.resetQueue(bufferSize: self.configuration.outboundQueueSize)
        if state.inboundStreams == 0 {
            self.peers[peer] = state
            self.removePeer(peer)
        } else {
            self.peers[peer] = state
        }
    }

    private func removePeer(_ peer: PeerID) {
        self.peers.removeValue(forKey: peer)?.continuation.finish()
        self.router.removePeer(peer)
    }

    private func openOutboundStream(_ protocolID: String, on connection: Connection) {
        guard let connection = connection as? BaseConnection else {
            self.logger.debug("Unable to open a `\(protocolID)` stream on a \(type(of: connection))")
            return
        }
        connection.newStream(forProtocol: protocolID, mode: .ifOutboundDoesntAlreadyExist)
    }

    /// The first RPC on each of our outbound streams announces all of our subscriptions
    private func helloFrame() -> ByteBuffer? {
        guard !self.topics.isEmpty else { return nil }
        let rpc = RPC.with {
            $0.subscriptions = self.topics.keys.map { topic in
                .with {
                    $0.topicID = topic
                    $0.subscribe = true
                }
            }
        }
        return (try? rpc.serializedData()).map { ByteBuffer(bytes: $0) }
    }

    // MARK: - Inbound RPCs

    /// Processes one inbound RPC (subscription changes, then control messages, then published messages).
    func handle(_ frame: ByteBuffer, from peer: PeerID) async {
        guard self.isRunning else { return }
        let rpc: RPC
        do {
            rpc = try RPC(serializedBytes: Array(frame.readableBytesView))
        } catch {
            self.logger.warning("Dropping an undecodable RPC from \(peer): \(error)")
            return
        }

        /// Like go-libp2p-pubsub, an RPC announcing more subscriptions than our filter allows is ignored entirely
        let filter = self.configuration.subscriptionFilter
        if let limit = filter.maxSubscriptionsPerRPC, rpc.subscriptions.count > limit {
            self.logger.warning("Dropping an RPC from \(peer) announcing \(rpc.subscriptions.count) subscriptions (limit \(limit))")
            return
        }

        for subscription in rpc.subscriptions where subscription.hasTopicID && filter.allows(subscription.topicID) {
            self.router.handleSubscription(from: peer, topic: subscription.topicID, subscribed: subscription.subscribe)
            if subscription.subscribe, let state = self.topics[subscription.topicID] {
                self.deliver(.newPeer(peer), to: state)
            }
        }

        if rpc.hasControl {
            let replies = self.router.handleControl(
                rpc.control,
                from: peer,
                hasSeen: { self.seen.contains($0) },
                now: self.clock.now
            )
            self.flush(replies)
        }

        if !rpc.msgs.isEmpty {
            await self.process(rpc.msgs, from: peer)
        }
    }

    private func process(_ messages: [RPC.Message], from peer: PeerID) async {
        var outbox = Outbox()
        for message in messages {
            /// We only process messages for topics we're subscribed to
            guard let topic = message.topicIds.first, let state = self.topics[topic] else { continue }

            if let violation = MessageSigning.check(message, against: state.signaturePolicy) {
                self.logger.debug(
                    "Dropping a `\(topic)` message from \(peer) that violates the signature policy: \(violation)"
                )
                continue
            }
            /// Drop our own messages if a peer echoes them back to us
            if !message.from.isEmpty, message.from == self.localPeer { continue }

            let id = state.messageID.id(for: message)
            guard !self.seen.contains(id) else { continue }

            /// Give the router a chance to act before validation (ex: telling our mesh peers not to send us duplicates)
            self.flush(self.router.received(message, id: id, topic: topic, from: peer))

            /// Validation happens off the actor (validators may be slow)
            let result = await Self.validate(
                message,
                from: peer,
                validators: state.registrations.values.map(\.validator),
                timeout: self.configuration.validationTimeout
            )
            guard result == .accept else {
                self.logger.debug("Dropping a `\(topic)` message from \(peer) that failed validation (\(result))")
                continue
            }
            /// Like go-libp2p-pubsub, a message is only marked as seen once it passes validation
            guard self.seen.insert(id, now: self.clock.now), let current = self.topics[topic] else { continue }

            self.deliver(.data(message), to: current)
            for target in self.router.route(message, id: id, topic: topic, from: peer, now: self.clock.now)
            where target != peer && !(message.from == target) {
                outbox.send(messages: [message], to: target)
            }
        }
        self.flush(outbox)
    }

    /// Runs a message through a topic's validators. Any rejection rejects the message, any ignore (or throttle) ignores it.
    private nonisolated static func validate(
        _ message: RPC.Message,
        from peer: PeerID,
        validators: [MessageValidator],
        timeout: Duration?
    ) async -> PubSub.ValidationResult {
        let runValidators: @Sendable () async -> PubSub.ValidationResult = {
            var result = PubSub.ValidationResult.accept
            for validator in validators {
                switch await validator.validate(message, from: peer) {
                case .accept: continue
                case .reject: return .reject
                case .ignore, .throttle: result = .ignore
                }
            }
            return result
        }
        guard let timeout else { return await runValidators() }

        return await withTaskGroup(of: PubSub.ValidationResult?.self) { group in
            group.addTask { await runValidators() }
            group.addTask {
                try? await Task.sleep(for: timeout)
                return nil
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            /// Timing out ignores the message
            return first ?? .ignore
        }
    }

    // MARK: - Heartbeat

    func heartbeat() {
        guard self.isRunning else { return }
        self.seen.prune(now: self.clock.now)
        self.flush(self.router.heartbeat(now: self.clock.now))
    }

    // MARK: - Outbound RPCs

    private func deliver(_ event: PubSub.SubscriptionEvent, to state: TopicState) {
        for registration in state.registrations.values { registration.deliver(event) }
    }

    private func flush(_ outbox: Outbox) {
        for (peer, rpc) in outbox.rpcs { self.send(rpc, to: peer) }
        for peer in outbox.dials { self.dial(peer) }
    }

    /// Connects to a peer the router asked for, unless we're already connected to (or dialing) it
    private func dial(_ peer: PeerID) {
        guard let dialer = self.dialer, peer != self.localPeer, self.peers[peer] == nil else { return }
        guard self.pendingDials.insert(peer).inserted else { return }
        self.logger.debug("Connecting to \(peer)")
        Task { [weak self] in
            await dialer(peer)
            await self?.dialFinished(peer)
        }
    }

    private func dialFinished(_ peer: PeerID) {
        self.pendingDials.remove(peer)
    }

    private func send(_ rpc: RPC, to peer: PeerID) {
        guard let state = self.peers[peer] else {
            self.logger.trace("Skip RPC for \(peer) that we're not connected to")
            return
        }
        let (fragments, dropped) = rpc.fragmented(maxSize: self.configuration.maxMessageSize)
        if dropped > 0 {
            self.logger.warning(
                "Dropped \(dropped) message(s) to \(peer) exceeding the max message size of \(self.configuration.maxMessageSize) bytes"
            )
        }
        for fragment in fragments {
            guard let bytes = try? fragment.serializedData() else { continue }
            if case .dropped = state.continuation.yield(ByteBuffer(bytes: bytes)) {
                self.logger.debug("Dropped an RPC to \(peer), its outbound queue is full")
            }
        }
    }
}

// MARK: - State

extension PubSubEngine {
    enum RegistrationKey: Hashable {

        /// A `PubSubSubscription`
        case subscription(UUID)

        /// A swift-libp2p-core `SubscriptionHandler` (one per topic)
        case legacyHandler

        /// A subscription without an event consumer (one per topic)
        case join

    }

    struct Registration {
        let validator: MessageValidator
        var events: AsyncStream<PubSub.SubscriptionEvent>.Continuation? = nil
        var legacyHandler: LegacySubscriptionHandler? = nil

        func deliver(_ event: PubSub.SubscriptionEvent) {
            self.events?.yield(event)
            self.legacyHandler?.deliver(event)
        }

        func finish() {
            self.events?.finish()
        }
    }

    struct TopicState {
        let signaturePolicy: PubSub.SignaturePolicy
        let messageID: MessageIDStrategy
        var registrations: [RegistrationKey: Registration]
    }

    struct PeerState {
        /// RPCs waiting to be written to the peer, consumed by the writer of our outbound stream
        var queue: AsyncStream<ByteBuffer>
        var continuation: AsyncStream<ByteBuffer>.Continuation
        /// Identifies the task currently writing to our outbound stream, if we have one
        var writer: UUID?
        var inboundStreams: Int = 0

        init(bufferSize: Int) {
            /// Like go-libp2p-pubsub, RPCs produced while the queue is full are dropped
            (self.queue, self.continuation) = AsyncStream.makeStream(bufferingPolicy: .bufferingOldest(bufferSize))
        }

        mutating func resetQueue(bufferSize: Int) {
            self.continuation.finish()
            (self.queue, self.continuation) = AsyncStream.makeStream(bufferingPolicy: .bufferingOldest(bufferSize))
        }
    }

    struct Writer: Sendable {
        let token: UUID
        let queue: AsyncStream<ByteBuffer>
        let hello: ByteBuffer?
    }
}

/// Bridges swift-libp2p-core's `SubscriptionHandler` into the engine.
///
/// - Note: `SubscriptionHandler` isn't `Sendable`, its `on` callback is assigned by the caller after subscribing.
///   We only ever read `on` in order to invoke it, which is the same contract core's API has always had.
final class LegacySubscriptionHandler: @unchecked Sendable {
    let handler: PubSub.SubscriptionHandler

    init(_ handler: PubSub.SubscriptionHandler) {
        self.handler = handler
    }

    func deliver(_ event: PubSub.SubscriptionEvent) {
        _ = self.handler.on?(event)
    }
}
