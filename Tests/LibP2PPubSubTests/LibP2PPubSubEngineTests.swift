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
import Testing

@testable import LibP2PPubSub

@Suite("Libp2p PubSub Engine Tests", .timeLimit(.minutes(5)), .serialized)
struct LibP2PPubSubEngineTests {

    // MARK: - Inbound pipeline

    /// Only messages that are well formed, correctly signed, unseen and valid are delivered
    @Test func testInboundMessagePipeline() async throws {
        let engine = try Self.makeEngine()
        await engine.start()

        let subscription = try await engine.subscribe(
            TopicConfiguration(
                topic: "fruit",
                validator: .predicate { String(decoding: $0.data, as: UTF8.self) != "bad" }
            )
        )

        let author = try PeerID(.Ed25519)
        let relay = try PeerID(.Ed25519)
        let good = try Self.signedMessage("good", by: author, seqno: 1)
        var tampered = try Self.signedMessage("tampered", by: author, seqno: 2)
        tampered.data = Data("tampered!".utf8)

        let rpc = try RPC.with {
            $0.subscriptions = [
                .with {
                    $0.topicID = "fruit"
                    $0.subscribe = true
                }
            ]
            $0.msgs = [
                good,
                try Self.signedMessage("bad", by: author, seqno: 3),
                good,  // duplicate
                tampered,
                try Self.signedMessage("elsewhere", topic: "news", by: author, seqno: 4),  // not subscribed
            ]
        }
        await engine.handle(try Self.frame(rpc), from: relay)
        /// The same message arriving again (via another peer) is a duplicate
        await engine.handle(try Self.frame(.with { $0.msgs = [good] }), from: author)

        let events = await Self.drain(subscription)
        #expect(Self.messages(in: events) == ["good"])
        #expect(events.contains { if case .newPeer(let peer) = $0 { return peer == relay } else { return false } })
        #expect(await engine.peers(subscribedTo: "fruit") == [relay])

        await engine.stop()
    }

    /// Rejected messages aren't marked as seen, so a valid copy arriving later (ex: once a validator's state changes) is still delivered
    @Test func testRejectedMessagesAreNotMarkedAsSeen() async throws {
        let engine = try Self.makeEngine()
        await engine.start()

        let accept = ManagedAtomicFlag()
        let subscription = try await engine.subscribe(
            TopicConfiguration(topic: "fruit", validator: .predicate { _ in accept.value })
        )
        let author = try PeerID(.Ed25519)
        let message = try Self.signedMessage("banana", by: author, seqno: 1)

        await engine.handle(try Self.frame(.with { $0.msgs = [message] }), from: author)
        accept.value = true
        await engine.handle(try Self.frame(.with { $0.msgs = [message] }), from: author)

        #expect(Self.messages(in: await Self.drain(subscription)) == ["banana"])
        await engine.stop()
    }

    /// Validators that take too long are ignored
    @Test func testValidationTimeout() async throws {
        let engine = try Self.makeEngine(configuration: .init(validationTimeout: .milliseconds(50)))
        await engine.start()

        let subscription = try await engine.subscribe(
            TopicConfiguration(
                topic: "fruit",
                validator: MessageValidator { message, _ in
                    if String(decoding: message.data, as: UTF8.self) == "slow" {
                        try? await Task.sleep(for: .seconds(5))
                    }
                    return .accept
                }
            )
        )
        let author = try PeerID(.Ed25519)
        let rpc = try RPC.with {
            $0.msgs = [
                try Self.signedMessage("slow", by: author, seqno: 1),
                try Self.signedMessage("fast", by: author, seqno: 2),
            ]
        }
        await engine.handle(try Self.frame(rpc), from: author)

        #expect(Self.messages(in: await Self.drain(subscription)) == ["fast"])
        await engine.stop()
    }

    /// Our own messages are delivered locally when `emitSelf` is set, and dropped if a peer echoes them back
    @Test func testEmitSelfAndEchoSuppression() async throws {
        let engine = try Self.makeEngine(configuration: .init(emitSelf: true))
        await engine.start()

        let subscription = try await engine.subscribe(TopicConfiguration(topic: "fruit"))
        try await engine.publish(Data("banana".utf8), to: "fruit")

        /// A peer echoing our own message back to us
        let echo = try Self.signedMessage("banana", by: engine.localPeer, seqno: 1)
        await engine.handle(try Self.frame(.with { $0.msgs = [echo] }), from: try PeerID(.Ed25519))

        #expect(Self.messages(in: await Self.drain(subscription)) == ["banana"])
        await engine.stop()
    }

    // MARK: - Subscriptions

    /// A topic stays joined while it has subscriptions, and is left once the last one ends
    @Test func testSubscriptionLifetime() async throws {
        let engine = try Self.makeEngine()
        await engine.start()

        let first = try await engine.subscribe(TopicConfiguration(topic: "fruit"))
        let second = try await engine.subscribe(TopicConfiguration(topic: "fruit"))
        #expect(await engine.subscribedTopics() == ["fruit"])

        first.cancel()
        try await Task.sleep(for: .milliseconds(50))
        #expect(await engine.subscribedTopics() == ["fruit"], "Still subscribed via the second subscription")

        second.cancel()
        #expect(await Self.eventually { await engine.subscribedTopics().isEmpty })

        await engine.stop()
    }

    /// Every subscription to a topic must use the same signature policy
    @Test func testConflictingSignaturePolicy() async throws {
        let engine = try Self.makeEngine()
        await engine.start()

        let subscription = try await engine.subscribe(TopicConfiguration(topic: "fruit", signaturePolicy: .strictSign))
        await #expect(throws: PubSubError.conflictingSignaturePolicy(topic: "fruit")) {
            _ = try await engine.subscribe(TopicConfiguration(topic: "fruit", signaturePolicy: .strictNoSign))
        }
        await #expect(throws: PubSubError.invalidTopic) {
            _ = try await engine.subscribe(TopicConfiguration(topic: ""))
        }

        subscription.cancel()
        await engine.stop()
    }

    /// Unsubscribing from a topic ends all of its subscriptions, and stopping the engine ends every subscription
    @Test func testUnsubscribeAndStopEndSubscriptions() async throws {
        let engine = try Self.makeEngine()
        await engine.start()

        let fruit = try await engine.subscribe(TopicConfiguration(topic: "fruit"))
        let news = try await engine.subscribe(TopicConfiguration(topic: "news"))

        await engine.unsubscribe(from: "fruit")
        for await _ in fruit {}
        #expect(await engine.subscribedTopics() == ["news"])

        await engine.stop()
        for await _ in news {}
        #expect(await engine.subscribedTopics().isEmpty)
        await #expect(throws: PubSubError.notRunning) {
            try await engine.publish(Data(), to: "news")
        }
    }

    // MARK: - Subscription filter

    /// We can't subscribe to topics our filter doesn't allow, and we don't track peers' subscriptions to them either
    @Test func testSubscriptionFilterAllowlist() async throws {
        let engine = try Self.makeEngine(configuration: .init(subscriptionFilter: .allowlist(["fruit"])))
        await engine.start()

        await #expect(throws: PubSubError.topicNotAllowed(topic: "news")) {
            _ = try await engine.subscribe(TopicConfiguration(topic: "news"))
        }

        let peer = try PeerID(.Ed25519)
        let rpc = RPC.with {
            $0.subscriptions = [
                .with {
                    $0.topicID = "fruit"
                    $0.subscribe = true
                },
                .with {
                    $0.topicID = "news"
                    $0.subscribe = true
                },
            ]
        }
        await engine.handle(try Self.frame(rpc), from: peer)
        #expect(await engine.peers(subscribedTo: "fruit") == [peer])
        #expect(await engine.peers(subscribedTo: "news").isEmpty)

        await engine.stop()
    }

    /// An RPC announcing more subscriptions than our filter allows is ignored entirely (including its messages)
    @Test func testSubscriptionFilterLimit() async throws {
        let engine = try Self.makeEngine(
            configuration: .init(subscriptionFilter: SubscriptionFilter(maxSubscriptionsPerRPC: 2) { _ in true })
        )
        await engine.start()
        let subscription = try await engine.subscribe(TopicConfiguration(topic: "fruit"))

        let author = try PeerID(.Ed25519)
        let rpc = try RPC.with {
            $0.subscriptions = ["a", "b", "fruit"].map { topic in
                .with {
                    $0.topicID = topic
                    $0.subscribe = true
                }
            }
            $0.msgs = [try Self.signedMessage("banana", by: author, seqno: 1)]
        }
        await engine.handle(try Self.frame(rpc), from: author)

        #expect(await engine.peers(subscribedTo: "fruit").isEmpty)
        #expect(Self.messages(in: await Self.drain(subscription)).isEmpty)
        await engine.stop()
    }

    // MARK: - Helpers

    /// The first message delivered to the subscription, or `nil` if none arrives within the timeout
    private static func firstMessage(
        in subscription: PubSubSubscription,
        timeout: Duration = .seconds(10)
    ) async throws -> String? {
        try await withThrowingTaskGroup(of: String?.self) { group in
            group.addTask {
                for await message in subscription.messages { return String(decoding: message.data, as: UTF8.self) }
                return nil
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                return nil
            }
            let first = try await group.next() ?? nil
            group.cancelAll()
            return first
        }
    }

    private static func makeEngine(
        router: any PubSubRouter = FloodSubRouter(),
        configuration: PubSubConfiguration = .init()
    ) throws -> PubSubEngine {
        var logger = Logger(label: "engine-tests")
        logger.logLevel = .critical
        return PubSubEngine(
            protocolIDs: [FloodSub.multicodec],
            localPeer: try PeerID(.Ed25519),
            configuration: configuration,
            router: router,
            logger: logger
        )
    }

    /// A StrictSign message authored (and signed) by `author`
    private static func signedMessage(
        _ data: String,
        topic: String = "fruit",
        by author: PeerID,
        seqno: UInt64
    ) throws -> RPC.Message {
        let message = RPC.Message.with {
            $0.data = Data(data.utf8)
            $0.topicIds = [topic]
            $0.from = Data(author.id)
            $0.seqno = withUnsafeBytes(of: seqno.bigEndian) { Data($0) }
        }
        return try MessageSigning.prepare(message, policy: .strictSign, signer: author)
    }

    private static func frame(_ rpc: RPC) throws -> ByteBuffer {
        ByteBuffer(bytes: try rpc.serializedData())
    }

    /// Ends the subscription and returns every event it buffered
    private static func drain(_ subscription: PubSubSubscription) async -> [PubSub.SubscriptionEvent] {
        subscription.cancel()
        var events: [PubSub.SubscriptionEvent] = []
        for await event in subscription { events.append(event) }
        return events
    }

    private static func messages(in events: [PubSub.SubscriptionEvent]) -> [String] {
        events.compactMap {
            guard case .data(let message) = $0 else { return nil }
            return String(decoding: message.data, as: UTF8.self)
        }
    }

    /// Polls `condition` until it holds, or the timeout elapses
    private static func eventually(
        timeout: Duration = .seconds(5),
        _ condition: @Sendable () async -> Bool
    ) async -> Bool {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if await condition() { return true }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return await condition()
    }
}

#if TestDependencies

import LibP2PNoise
import LibP2PYAMUX

extension LibP2PPubSubEngineTests {

    // MARK: - Async API (network)

    /// Two GossipSub nodes exchanging messages via the async subscription API
    @Test(.timeLimit(.minutes(1)))
    func testAsyncSubscriptionAPI() async throws {
        let node1 = try await Self.makeHost()
        let node2 = try await Self.makeHost()
        try await node1.startup()
        try await node2.startup()

        do {
            let subscription = try await node2.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
            try await node1.newStream(to: node2.peerInfo, forProtocol: GossipSub.multicodec)

            /// Wait for node1 to learn about node2's subscription
            #expect(
                await Self.eventually { await node1.pubsub.gossipsub.peers(subscribedTo: "fruit") == [node2.peerID] }
            )

            /// Both nodes speak GossipSub v1.2, so that's what they negotiate
            #expect(
                await Self.eventually {
                    await node1.pubsub.gossipsub.engine.inspectRouter { router in
                        (router as? GossipSubRouter)?.peers[node2.peerID]?.protocolKind == .gossipSubV1_2
                    }
                }
            )

            /// node1 isn't subscribed, but can still publish to the topic
            try await node1.pubsub.gossipsub.publish(Data("banana".utf8), to: "fruit")

            let received = try await withThrowingTaskGroup(of: String?.self) { group in
                group.addTask {
                    for await message in subscription.messages { return String(decoding: message.data, as: UTF8.self) }
                    return nil
                }
                group.addTask {
                    try await Task.sleep(for: .seconds(10))
                    return nil
                }
                let first = try await group.next() ?? nil
                group.cancelAll()
                return first
            }
            #expect(received == "banana")

            /// Ending the subscription unsubscribes node2 from the topic, which node1 hears about
            subscription.cancel()
            #expect(await Self.eventually { await node1.pubsub.gossipsub.peers(subscribedTo: "fruit").isEmpty })
        } catch {
            Issue.record(error)
        }

        try await node1.asyncShutdown()
        try await node2.asyncShutdown()
    }

    /// Two GossipSub nodes with peer scoring enabled. Tests messages flow, and that the receiver credits the sender's first delivery
    @Test(.timeLimit(.minutes(1)))
    func testPeerScoringOverTheNetwork() async throws {
        let scoring = try GossipSubScoring(parameters: .init(topics: ["fruit": TopicScoreParameters()]))
        let node1 = try await Self.makeHost(.gossipsub(configuration: .init(), parameters: .init(scoring: scoring)))
        let node2 = try await Self.makeHost(.gossipsub(configuration: .init(), parameters: .init(scoring: scoring)))
        try await node1.startup()
        try await node2.startup()

        do {
            let subscription1 = try await node1.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
            let subscription2 = try await node2.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
            try await node1.newStream(to: node2.peerInfo, forProtocol: GossipSub.multicodec)
            #expect(
                await Self.eventually { await node1.pubsub.gossipsub.peers(subscribedTo: "fruit") == [node2.peerID] }
            )

            try await node1.pubsub.gossipsub.publish(Data("banana".utf8), to: "fruit")
            #expect(try await Self.firstMessage(in: subscription2) == "banana")

            let score = await node2.pubsub.gossipsub.engine.inspectRouter { router in
                (router as? GossipSubRouter)?.score(of: node1.peerID) ?? 0
            }
            #expect(score > 0)
            subscription1.cancel()
            subscription2.cancel()
        } catch {
            Issue.record(error)
        }

        try await node1.asyncShutdown()
        try await node2.asyncShutdown()
    }

    /// With peer exchange enabled, we collect the signed peer records our peers send us when they're identified
    @Test(.timeLimit(.minutes(1)))
    func testSignedPeerRecordsFromIdentify() async throws {
        let node1 = try await Self.makeHost(.gossipsub(configuration: .init(), parameters: .init(peerExchange: true)))
        let node2 = try await Self.makeHost(.gossipsub)
        try await node1.startup()
        try await node2.startup()

        do {
            try await node1.newStream(to: node2.peerInfo, forProtocol: GossipSub.multicodec)
            let recorded = await Self.eventually {
                await node1.pubsub.gossipsub.engine.inspectRouter { router in
                    (router as? GossipSubRouter)?.signedRecords[node2.peerID] != nil
                }
            }
            #expect(recorded)
        } catch {
            Issue.record(error)
        }

        try await node1.asyncShutdown()
        try await node2.asyncShutdown()
    }

    /// A GossipSub node and a FloodSub-only node exchanging messages (GossipSub speaks `/floodsub/1.0.0` to FloodSub peers)
    @Test(.timeLimit(.minutes(1)))
    func testGossipSubInteroperatesWithFloodSub() async throws {
        let gossipNode = try await Self.makeHost(.gossipsub)
        let floodNode = try await Self.makeHost(.floodsub)
        try await gossipNode.startup()
        try await floodNode.startup()

        do {
            let gossipSubscription = try await gossipNode.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
            let floodSubscription = try await floodNode.pubsub.floodsub.subscribe(TopicConfiguration(topic: "fruit"))

            /// The FloodSub node dials the GossipSub node over `/floodsub/1.0.0`
            try await floodNode.newStream(to: gossipNode.peerInfo, forProtocol: FloodSub.multicodec)
            #expect(
                await Self.eventually {
                    await gossipNode.pubsub.gossipsub.peers(subscribedTo: "fruit") == [floodNode.peerID]
                }
            )
            #expect(
                await Self.eventually {
                    await floodNode.pubsub.floodsub.peers(subscribedTo: "fruit") == [gossipNode.peerID]
                }
            )

            /// The FloodSub peer is never grafted into our mesh
            let notMeshed = await gossipNode.pubsub.gossipsub.engine.inspectRouter { router in
                (router as? GossipSubRouter)?.mesh["fruit"]?.isEmpty ?? false
            }
            #expect(notMeshed)

            try await gossipNode.pubsub.gossipsub.publish(Data("from gossipsub".utf8), to: "fruit")
            try await floodNode.pubsub.floodsub.publish(Data("from floodsub".utf8), to: "fruit")

            #expect(try await Self.firstMessage(in: floodSubscription) == "from gossipsub")
            #expect(try await Self.firstMessage(in: gossipSubscription) == "from floodsub")
        } catch {
            Issue.record(error)
        }

        try await gossipNode.asyncShutdown()
        try await floodNode.asyncShutdown()
    }

    private static func makeHost(_ router: Application.PubSubServices.Provider = .gossipsub) async throws -> Application
    {
        let lib = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        lib.connectionManager.use(connectionType: BaseConnection.self)
        lib.logger.logLevel = .info
        lib.security.use(.noise)
        lib.muxers.use(.yamux)
        lib.pubsub.use(router)
        lib.servers.use(.tcp(host: "127.0.0.1", port: 0))
        return lib
    }
}

#endif

/// A thread safe boolean for flipping validator behaviour mid-test
private final class ManagedAtomicFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = false
    var value: Bool {
        get { lock.withLock { _value } }
        set { lock.withLock { _value = newValue } }
    }
}
