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
import LibP2PNoise
import LibP2PYAMUX
import NIOConcurrencyHelpers
import Testing

@testable import LibP2PPubSub

@Suite("Libp2p PubSub Regression Tests", .timeLimit(.minutes(5)), .serialized)
final class LibP2PPubSubRegressionTests {

    // MARK: - Helpers

    private static func makeMessage(
        data: String,
        topics: [String] = ["fruit"],
        from: Data? = nil,
        seqno: Data? = nil
    ) -> RPC.Message {
        RPC.Message.with { msg in
            msg.data = Data(data.utf8)
            msg.topicIds = topics
            if let from { msg.from = from }
            if let seqno { msg.seqno = seqno }
        }
    }

    /// Creates an (unstarted) floodsub enabled Application with an Ed25519 PeerID
    private func makeFloodsubApp() async throws -> Application {
        let app = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        app.logger.logLevel = .info
        app.pubsub.use(.floodsub)
        return app
    }

    // MARK: - GossipSub MessageCache

    /// mcache_len = 5, mcache_gossip = 3. Messages should be gossiped for 3 shifts and retrievable for 5.
    @Test(.timeLimit(.minutes(1)))
    func testMessageCacheWindows() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let mcache = MessageCache(eventLoop: elg.next(), historyWindows: 5, gossipWindows: 3)
        let msg = Self.makeMessage(data: "banana")
        let id = Data("id-1".utf8)

        /// Storing immediately after init must not crash (previously `windows[0] =` on an empty array)
        let stored = try await mcache.put(messages: [id: msg], on: nil).get()
        #expect(stored.count == 1)
        /// Duplicate puts are rejected
        #expect(try await mcache.put(messages: [id: msg], on: nil).get().isEmpty)

        #expect(try await mcache.getGossipIDs(topic: "fruit").get() == [id])
        #expect(try await mcache.getGossipIDs(topic: "other").get().isEmpty)

        /// Each heartbeat shifts the cache by one window
        for _ in 0..<2 { try await mcache.heartbeat().get() }
        #expect(try await mcache.getGossipIDs(topic: "fruit").get() == [id], "Still inside the gossip window")

        try await mcache.heartbeat().get()
        #expect(try await mcache.getGossipIDs(topic: "fruit").get().isEmpty, "Outside of the gossip window")
        #expect(try await mcache.get(messageID: id).get() != nil, "Still inside the history window")

        for _ in 0..<2 { try await mcache.heartbeat().get() }
        #expect(try await mcache.get(messageID: id).get() == nil, "Evicted after mcache_len heartbeats")

        try await elg.shutdownGracefully()
    }

    // MARK: - Floodsub BasicMessageCache

    /// Messages stored via the batch `put(messages:)` API must expire (previously they were never evicted)
    @Test(.timeLimit(.minutes(1)))
    func testBasicMessageCacheBatchPutExpires() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let cache = BasicMessageCache(eventLoop: elg.next(), timeToLiveInSeconds: 0.05)
        let id = Data("id-1".utf8)

        _ = try await cache.put(messages: [id: Self.makeMessage(data: "banana")], on: nil).get()
        #expect(try await cache.exists(messageID: id).get())

        try await Task.sleep(for: .milliseconds(100))
        try await cache.heartbeat().get()
        #expect(try await cache.exists(messageID: id).get() == false)

        try await elg.shutdownGracefully()
    }

    // MARK: - Peer State

    /// Disconnected peers must be removed from our mesh & fanout, otherwise they inflate our mesh degree
    @Test(.timeLimit(.minutes(1)))
    func testGossipsubPeeringStateDisconnectCleansMesh() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ps = PeeringState(eventLoop: elg.next())
        try ps.start()
        let meshPeer = try PeerID(.Ed25519)
        let fanoutPeer = try PeerID(.Ed25519)

        _ = try await ps.addNewPeer(meshPeer, on: nil).get()
        _ = try await ps.addNewPeer(fanoutPeer, on: nil).get()
        _ = try await ps.subscribeSelf(to: "fruit", on: nil).get()
        try await ps.update(subscriptions: ["fruit": true, "news": true], for: meshPeer, on: nil).get()
        try await ps.update(subscriptions: ["news": true], for: fanoutPeer, on: nil).get()

        /// Subscribing no longer auto promotes known peers into the mesh, they need to be grafted
        #expect(try await ps.meshDegrees().get() == ["fruit": 0])
        try await ps.makeFullPeer(meshPeer, for: "fruit").get()
        #expect(try await ps.meshDegrees().get() == ["fruit": 1])

        /// isFullPeer is topic specific and doesn't throw for unknown peers
        #expect(try await ps.isFullPeer(meshPeer, forTopic: "fruit").get())
        #expect(try await ps.isFullPeer(meshPeer, forTopic: "news").get() == false)
        #expect(try await ps.isFullPeer(try PeerID(.Ed25519), forTopic: "fruit").get() == false)

        /// A duplicate subscription announcement shouldn't place a mesh peer into fanout as well
        try await ps.update(subscriptions: ["fruit": true], for: meshPeer, on: nil).get()
        #expect(try await ps.metaPeerIDs().get()["fruit"]?.isEmpty ?? true)

        try await ps.onPeerDisconnected(meshPeer).get()
        #expect(try await ps.meshDegrees().get() == ["fruit": 0])
        #expect(try await ps.metaPeerIDs().get()["news"]?.map { $0.b58String } == [fanoutPeer.b58String])

        try await ps.onPeerDisconnected(fanoutPeer).get()
        #expect(try await ps.metaPeerIDs().get()["news"] == nil)

        try await elg.shutdownGracefully()
    }

    /// Floodsub floods to every known topic peer (even for topics we're not subscribed to) and cleans up on disconnect
    @Test(.timeLimit(.minutes(1)))
    func testFloodsubPeerStateFloodTargetsAndDisconnect() async throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let ps = BasicPeerState(eventLoop: elg.next())
        let peer = try PeerID(.Ed25519)

        _ = try await ps.addNewPeer(peer, on: nil).get()
        try await ps.update(subscriptions: ["news": true], for: peer, on: nil).get()

        /// We're not subscribed to `news`, but we should still be able to publish to the peers that are
        let targets: [PubSub.Subscriber] = try await ps.peersSubscribedTo(topic: "news", on: nil).get()
        #expect(targets.map { $0.id } == [peer])

        /// Unsubscribing before `start()` should still clean up our mesh entry
        _ = try await ps.subscribeSelf(to: "fruit", on: nil).get()
        _ = try await ps.unsubscribeSelf(from: "fruit", on: nil).get()
        #expect(try await ps.topicSubscriptions().get().isEmpty)

        try await ps.onPeerDisconnected(peer).get()
        let remaining: [PubSub.Subscriber] = try await ps.peersSubscribedTo(topic: "news", on: nil).get()
        #expect(remaining.isEmpty)

        try await elg.shutdownGracefully()
    }

    // MARK: - Signing & Signature Policies

    /// Ed25519 PeerIDs inline their public key, so (like go / rust) we omit the `key` field and the receiver extracts the key from `from`
    @Test(.timeLimit(.minutes(1)))
    func testStrictSignWithInlinedKey() async throws {
        let app = try await makeFloodsubApp()
        let fsub = app.pubsub.floodsub
        fsub.assignSignaturePolicy(for: "fruit", policy: .strictSign)

        let msg = Self.makeMessage(
            data: "banana",
            from: Data(fsub.peerID.id),
            seqno: Data(fsub.nextMessageSequenceNumber())
        )
        let signed = try fsub.prepareOutboundMessage(msg, policy: .strictSign)

        #expect(!signed.signature.isEmpty)
        #expect(signed.hasKey == false, "Ed25519 keys are inlined in the PeerID, the key field should be omitted")
        #expect(fsub.passesMessageSignaturePolicy(signed))

        /// Tampering with the payload invalidates the signature
        var tampered = signed
        tampered.data = Data("pineapple".utf8)
        #expect(fsub.passesMessageSignaturePolicy(tampered) == false)

        /// An unsigned message is rejected under StrictSign
        #expect(fsub.passesMessageSignaturePolicy(msg) == false)

        try await app.asyncShutdown()
    }

    /// When the `key` field is present it must belong to the `from` PeerID
    @Test(.timeLimit(.minutes(1)))
    func testStrictSignRejectsMismatchedKey() async throws {
        let app = try await makeFloodsubApp()
        let fsub = app.pubsub.floodsub
        fsub.assignSignaturePolicy(for: "fruit", policy: .strictSign)

        let msg = Self.makeMessage(
            data: "banana",
            from: Data(fsub.peerID.id),
            seqno: Data(fsub.nextMessageSequenceNumber())
        )

        /// Including our own (matching) key is allowed
        var withKey = try fsub.prepareOutboundMessage(msg, policy: .strictSign)
        withKey.key = try Data(fsub.peerID.marshalPublicKey())
        #expect(fsub.passesMessageSignaturePolicy(withKey))

        /// Someone else's key is not
        var wrongKey = withKey
        wrongKey.key = try Data(PeerID(.Ed25519).marshalPublicKey())
        #expect(fsub.passesMessageSignaturePolicy(wrongKey) == false)

        try await app.asyncShutdown()
    }

    /// StrictNoSign messages must omit (and receivers must reject) the `from`, `seqno`, `signature` and `key` fields
    @Test(.timeLimit(.minutes(1)))
    func testStrictNoSign() async throws {
        let app = try await makeFloodsubApp()
        let fsub = app.pubsub.floodsub
        fsub.assignSignaturePolicy(for: "fruit", policy: .strictNoSign)

        let msg = Self.makeMessage(
            data: "banana",
            from: Data(fsub.peerID.id),
            seqno: Data(fsub.nextMessageSequenceNumber())
        )
        let prepared = try fsub.prepareOutboundMessage(msg, policy: .strictNoSign)
        #expect(!prepared.hasFrom && !prepared.hasSeqno && !prepared.hasSignature && !prepared.hasKey)
        #expect(fsub.passesMessageSignaturePolicy(prepared))

        /// Messages containing authorship info are rejected
        #expect(fsub.passesMessageSignaturePolicy(msg) == false)

        /// The from+seqno ID functions would collide under StrictNoSign, so a content based ID is substituted
        let idFunc = fsub.resolveMessageIDFunction(
            for: .init(
                topic: "fruit",
                signaturePolicy: .strictNoSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        let other = try fsub.prepareOutboundMessage(Self.makeMessage(data: "pineapple"), policy: .strictNoSign)
        #expect(!idFunc(prepared).isEmpty)
        #expect(idFunc(prepared) != idFunc(other))

        try await app.asyncShutdown()
    }

    /// A message claiming multiple topics could bypass a topic's policy / validators, so it's rejected outright
    @Test(.timeLimit(.minutes(1)))
    func testMultiTopicMessagesAreRejected() async throws {
        let app = try await makeFloodsubApp()
        let fsub = app.pubsub.floodsub
        fsub.assignSignaturePolicy(for: "fruit", policy: .strictNoSign)
        fsub.assignSignaturePolicy(for: "victim", policy: .strictNoSign)

        #expect(fsub.passesMessageSignaturePolicy(Self.makeMessage(data: "banana", topics: ["fruit"])))
        #expect(fsub.passesMessageSignaturePolicy(Self.makeMessage(data: "banana", topics: ["fruit", "victim"])) == false)
        #expect(fsub.passesMessageSignaturePolicy(Self.makeMessage(data: "banana", topics: [])) == false)

        try await app.asyncShutdown()
    }

    // MARK: - GossipSub Subscriptions

    /// Unsubscribing from a topic with an empty mesh used to be a silent no-op
    @Test(.timeLimit(.minutes(1)))
    func testGossipsubUnsubscribeWithEmptyMesh() async throws {
        let app = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        app.logger.logLevel = .info
        app.pubsub.use(.gossipsub)
        try await app.startup()

        let gsub = app.pubsub.gossipsub
        let _: PubSub.SubscriptionHandler = try gsub.subscribe(
            .init(topic: "fruit", signaturePolicy: .strictSign, validator: .acceptAll, messageIDFunc: .concatFromAndSequenceFields)
        )
        try await Task.sleep(for: .milliseconds(100))
        #expect(try await gsub.getTopics().contains("fruit"))

        try await gsub.unsubscribe(topic: "fruit", on: nil).get()
        #expect(try await gsub.getTopics().contains("fruit") == false)
        #expect(try await gsub.eventLoop.submit { gsub.subscriptions["fruit"] == nil }.get())

        /// Resubscribing works
        let _: PubSub.SubscriptionHandler = try gsub.subscribe(
            .init(topic: "fruit", signaturePolicy: .strictSign, validator: .acceptAll, messageIDFunc: .concatFromAndSequenceFields)
        )
        try await Task.sleep(for: .milliseconds(100))
        #expect(try await gsub.getTopics().contains("fruit"))

        try await app.asyncShutdown()
    }

    // MARK: - Validation (network)

    /// Messages that fail a topic's validator must be neither delivered nor forwarded.
    ///
    /// Network: node0 -> node1 -> node2. Every node rejects messages containing "bad".
    /// node0 publishes "bad" followed by "good". node1 & node2 should only ever see "good".
    @Test(.timeLimit(.minutes(1)))
    func testFloodsubValidatorsRejectMessages() async throws {
        let nodes = try await (0..<3).asyncMap { _ in try await self.makeHost() }
        let goodReceived = nodes.map { _ in AsyncSemaphore(value: 0) }
        let badReceived = NIOLockedValueBox<Int>(0)

        var subscriptions: [PubSub.SubscriptionHandler] = []
        for (idx, node) in nodes.enumerated() {
            let sub: PubSub.SubscriptionHandler = try node.pubsub.floodsub.subscribe(
                .init(
                    topic: "fruit",
                    signaturePolicy: .strictSign,
                    validator: .custom({ String(data: $0.data, encoding: .utf8) != "bad" }),
                    messageIDFunc: .concatFromAndSequenceFields
                )
            )
            sub.on = { event in
                if case .data(let msg) = event {
                    switch String(data: msg.data, encoding: .utf8) {
                    case "good": goodReceived[idx].signal()
                    default: badReceived.withLockedValue { $0 += 1 }
                    }
                }
                return node.eventLoopGroup.next().makeSucceededVoidFuture()
            }
            subscriptions.append(sub)
        }

        for node in nodes { try await node.startup() }

        do {
            try await nodes[0].newStream(to: nodes[1].peerInfo, forProtocol: FloodSub.multicodec)
            try await nodes[1].newStream(to: nodes[2].peerInfo, forProtocol: FloodSub.multicodec)

            /// Give the subscriptions a moment to propagate
            try await Task.sleep(for: .seconds(1))

            try await subscriptions[0].publish(Data("bad".utf8))
            try await subscriptions[0].publish(Data("good".utf8))

            try await goodReceived[1].wait(timeout: .seconds(10))
            try await goodReceived[2].wait(timeout: .seconds(10))

            /// Give any (incorrectly) forwarded "bad" messages a chance to arrive
            try await Task.sleep(for: .milliseconds(500))
            #expect(badReceived.withLockedValue { $0 } == 0)
        } catch {
            Issue.record(error)
        }

        for node in nodes { try await node.asyncShutdown() }
    }

    private func makeHost() async throws -> Application {
        let lib = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        lib.connectionManager.use(connectionType: BaseConnection.self)
        lib.logger.logLevel = .info
        lib.security.use(.noise)
        lib.muxers.use(.yamux)
        lib.pubsub.use(.floodsub)
        lib.servers.use(.tcp(host: "127.0.0.1", port: 0))
        return lib
    }
}

extension Sequence {
    fileprivate func asyncMap<T>(_ transform: (Element) async throws -> T) async rethrows -> [T] {
        var results: [T] = []
        for element in self { results.append(try await transform(element)) }
        return results
    }
}
