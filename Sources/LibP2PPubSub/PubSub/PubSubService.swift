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
import NIOConcurrencyHelpers

/// The functionality shared by ``FloodSub`` and ``GossipSub``.
///
/// The primary API is `async`:
/// ```swift
/// let subscription = try await app.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
/// try await app.pubsub.gossipsub.publish(Data("banana".utf8), to: "fruit")
/// for await message in subscription.messages { ... }
/// ```
///
/// The `EventLoopFuture` and `SubscriptionHandler` based API required by swift-libp2p-core's `PubSubCore` is also supported,
/// so routers keep working with `app.pubsub`.
///
/// - Note: Every operation is applied in the order it was issued, even when issued from synchronous code
///   (ex: a `SubscriptionHandler` returned by `subscribe(_:)` followed by `unsubscribe(topic:on:)`).
public class PubSubService: @unchecked Sendable {
    /// The event loop our `EventLoopFuture`s are completed on (unless another is requested)
    public let eventLoop: EventLoop

    public var state: ServiceLifecycleState {
        self.lifecycleState.withLockedValue { $0 }
    }

    let engine: PubSubEngine

    private let lifecycleState: NIOLockedValueBox<ServiceLifecycleState>

    /// The most recently issued engine operation, each new operation waits for it to complete
    private let lastOperation: NIOLockedValueBox<Task<Void, Never>?>

    /// Subscribes to our application's peer protocol changes (filtering peers for those who speak our protocols)
    private let peerEvents: @Sendable () -> AsyncStream<EventBus.EventEmitter>?

    /// Registers a router's protocol route, ex: `app.group("floodsub") { $0.on("1.0.0", handlers: handlers, use: handler) }`
    typealias RouteRegistration = (
        _ application: Application,
        _ handlers: [Application.ChildChannelHandlers.Provider],
        _ handler: @escaping @Sendable (LibP2PStream) async throws -> Void
    ) -> Void

    /// - Parameters:
    ///   - protocolIDs: The protocols we speak, in order of preference. We open our stream to each peer using the most
    ///     preferred protocol it supports.
    ///   - knownAddresses: Addresses to dial when the router asks us to connect to these peers (ex: direct peers). Other
    ///     peers are dialed using the addresses in our peer store.
    ///   - registerRoute: Registers a route for each of `protocolIDs`, all handled by the provided handler.
    init(
        application: Application,
        protocolIDs: [String],
        name: String,
        configuration: PubSubConfiguration,
        router: any PubSubRouter,
        knownAddresses: [PeerID: Multiaddr] = [:],
        registerRoute: RouteRegistration
    ) {
        var logger = Logger(label: "\(name)[\(application.peerID.shortDescription)]")
        logger.logLevel = application.logger.logLevel

        /// Connects to a peer the router asked for (ex: a direct peer, or one suggested via peer exchange).
        /// Once the peer's identified, our discovery opens a stream using the best protocol it supports, exactly as it
        /// does for any other peer.
        /// A verified signed peer record (from PX) is added to our peer store first (when present), so we know where to
        /// find the peer (and can pass its record on to others during peer exchange).
        let dialLogger = logger
        let dialer: @Sendable (PeerID, SealedEnvelope?) async -> Void = { [weak application] peer, record in
            guard let application else { return }
            do {
                if let record { try await application.peers.add(signedRecord: record) }
                if let address = knownAddresses[peer] {
                    try await application.connect(to: address)
                } else {
                    try await application.connect(to: peer)
                }
            } catch {
                dialLogger.debug("Failed to connect to \(peer): \(error)")
            }
        }

        /// Our peer store holds the signed peer records identify verified, we pass them on via peer exchange
        let signedPeerRecord: @Sendable (PeerID) async -> SealedEnvelope? = { [weak application] peer in
            /// The peer store throws for peers it doesn't know
            try? await application?.peers.getMostRecentSignedRecord(forPeer: peer)
        }

        let engine = PubSubEngine(
            protocolIDs: protocolIDs,
            localPeer: application.peerID,
            configuration: configuration,
            router: router,
            logger: logger,
            dialer: dialer,
            signedPeerRecord: signedPeerRecord
        )
        self.engine = engine
        self.eventLoop = application.eventLoopGroup.next()
        self.lifecycleState = .init(.stopped)
        self.lastOperation = .init(nil)

        /// Our streams are driven by the engine, framed with unsigned varint length prefixes
        registerRoute(application, [.varIntFramed(maxMessageLength: configuration.maxMessageSize)]) {
            [weak engine] stream in
            await engine?.run(stream)
        }

        /// Learn about peers that support our protocols as they're identified. The engine consumes these events from a
        /// single stream (so they're handled in order) for as long as it's running, and the subscription ends when it stops.
        self.peerEvents = { [weak application] in
            application?.events.subscribe(to: [.remotePeerProtocolChange])
        }
    }

    // MARK: - Async API

    /// Subscribes to a topic, returning a ``PubSubSubscription`` that delivers the topic's events
    public func subscribe(_ configuration: TopicConfiguration) async throws -> PubSubSubscription {
        try await self.schedule { try await $0.subscribe(configuration) }.value
    }

    /// Subscribes to a topic described by a swift-libp2p-core `PubSub.SubscriptionConfig` (`AsyncPubSub`)
    @discardableResult
    public func subscribe(_ config: PubSub.SubscriptionConfig) async throws -> PubSub.Subscription {
        try await self.subscribe(TopicConfiguration(config))
    }

    /// Unsubscribes from a topic entirely, ending all of its subscriptions
    public func unsubscribe(from topic: String) async {
        _ = await self.schedule { await $0.unsubscribe(from: topic) }.result
    }

    /// Publishes `data` to `topic`. We needn't be subscribed to the topic.
    public func publish(_ data: Data, to topic: String) async throws {
        try await self.schedule { try await $0.publish(data, to: topic) }.value
    }

    /// The topics we're subscribed to
    public func subscribedTopics() async -> [String] {
        (try? await self.schedule { await $0.subscribedTopics() }.value) ?? []
    }

    /// The peers we know to be subscribed to `topic`
    public func peers(subscribedTo topic: String) async -> [PeerID] {
        (try? await self.schedule { await $0.peers(subscribedTo: topic) }.value) ?? []
    }

    // MARK: - PubSubCore

    public func start() throws {
        self.lifecycleState.withLockedValue { $0 = .started }
        let peerEvents = self.peerEvents
        self.schedule { await $0.start(peerEvents: peerEvents()) }
    }

    public func stop() throws {
        self.lifecycleState.withLockedValue { $0 = .stopped }
        self.schedule { await $0.stop() }
    }

    public func subscribe(_ config: PubSub.SubscriptionConfig, on loop: EventLoop? = nil) -> EventLoopFuture<Void> {
        let configuration = TopicConfiguration(config)
        return self.future(on: loop) { try await $0.join(configuration) }
    }

    /// Subscribes to a topic, delivering its events to the returned handler's `on` callback.
    ///
    /// - Note: Assign the handler's `on` callback promptly, events that arrive before it's assigned are dropped.
    public func subscribe(_ config: PubSub.SubscriptionConfig) throws -> PubSub.SubscriptionHandler {
        guard !config.topic.isEmpty else { throw PubSubError.invalidTopic }
        guard let pubsub = self as? PubSubCore else { throw PubSubError.notRunning }
        let handler = PubSub.SubscriptionHandler(pubSub: pubsub, topic: config.topic)
        let configuration = TopicConfiguration(config)
        self.schedule { engine in
            do {
                try await engine.subscribe(configuration, handler: handler)
            } catch {
                engine.logger.warning("Failed to subscribe to `\(configuration.topic)`: \(error)")
            }
        }
        return handler
    }

    public func unsubscribe(topic: String, on loop: EventLoop? = nil) -> EventLoopFuture<Void> {
        self.future(on: loop) { await $0.unsubscribe(from: topic) }
    }

    public func getTopics(on loop: EventLoop? = nil) -> EventLoopFuture<[String]> {
        self.future(on: loop) { await $0.subscribedTopics() }
    }

    public func getPeersSubscribed(to topic: String, on loop: EventLoop? = nil) -> EventLoopFuture<[PeerID]> {
        self.future(on: loop) { await $0.peers(subscribedTo: topic) }
    }

    public func publish(topic: String, data: Data, on loop: EventLoop? = nil) -> EventLoopFuture<Void> {
        self.future(on: loop) { try await $0.publish(data, to: topic) }
    }

    public func publish(topic: String, bytes: [UInt8], on loop: EventLoop? = nil) -> EventLoopFuture<Void> {
        self.publish(topic: topic, data: Data(bytes), on: loop)
    }

    public func publish(topic: String, buffer: ByteBuffer, on loop: EventLoop? = nil) -> EventLoopFuture<Void> {
        self.publish(topic: topic, data: Data(buffer.readableBytesView), on: loop)
    }

    // MARK: - LifecycleHandler

    public func didBoot(_ application: Application) throws {
        try self.start()
    }

    public func didBootAsync(_ application: Application) async throws {
        self.lifecycleState.withLockedValue { $0 = .started }
        let peerEvents = self.peerEvents
        _ = await self.schedule { await $0.start(peerEvents: peerEvents()) }.result
    }

    public func shutdown(_ application: Application) {
        try? self.stop()
    }

    public func shutdownAsync(_ application: Application) async {
        self.lifecycleState.withLockedValue { $0 = .stopped }
        _ = await self.schedule { await $0.stop() }.result
    }

    // MARK: - Operations

    /// Runs `operation` on the engine once every previously issued operation has completed
    @discardableResult
    func schedule<T: Sendable>(
        _ operation: @escaping @Sendable (PubSubEngine) async throws -> T
    ) -> Task<T, Error> {
        let engine = self.engine
        return self.lastOperation.withLockedValue { last in
            let previous = last
            let task = Task {
                await previous?.value
                return try await operation(engine)
            }
            last = Task { _ = await task.result }
            return task
        }
    }

    /// Schedules `operation` and bridges its result into an `EventLoopFuture`
    private func future<T: Sendable>(
        on loop: EventLoop?,
        _ operation: @escaping @Sendable (PubSubEngine) async throws -> T
    ) -> EventLoopFuture<T> {
        let task = self.schedule(operation)
        return (loop ?? self.eventLoop).makeFutureWithTask { try await task.value }
    }
}
