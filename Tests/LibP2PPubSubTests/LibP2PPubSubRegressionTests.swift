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

    private static func seqno(_ value: UInt64) -> Data {
        withUnsafeBytes(of: value.bigEndian) { Data($0) }
    }

    // MARK: - GossipSub MessageCache

    /// mcache_len = 5, mcache_gossip = 3. Messages should be gossiped for 3 shifts and retrievable for 5.
    @Test func testMessageCacheWindows() {
        var mcache = MessageCache(historyLength: 5, gossipLength: 3)
        let msg = Self.makeMessage(data: "banana")
        let id = Data("id-1".utf8)

        /// Storing immediately after init must work (previously `windows[0] =` on an empty array)
        let stored = mcache.put(id, message: msg, topic: "fruit")
        #expect(stored)
        /// Duplicate puts are rejected
        let storedAgain = mcache.put(id, message: msg, topic: "fruit")
        #expect(storedAgain == false)

        #expect(mcache.gossipIDs(for: "fruit") == [id])
        #expect(mcache.gossipIDs(for: "other").isEmpty)

        /// Each heartbeat shifts the cache by one window
        for _ in 0..<2 { mcache.shift() }
        #expect(mcache.gossipIDs(for: "fruit") == [id], "Still inside the gossip window")

        mcache.shift()
        #expect(mcache.gossipIDs(for: "fruit").isEmpty, "Outside of the gossip window")
        #expect(mcache.get(id) != nil, "Still inside the history window")

        for _ in 0..<2 { mcache.shift() }
        #expect(mcache.get(id) == nil, "Evicted after mcache_len shifts")
        #expect(mcache.count == 0)
    }

    /// The GossipSub router shifts its message cache on every heartbeat (it used to shift every other heartbeat)
    @Test func testGossipsubShiftsEveryHeartbeat() {
        var router = GossipSubRouter(parameters: .init(historyLength: 2, historyGossip: 1))
        let msg = Self.makeMessage(data: "banana")
        let id = Data("id-1".utf8)
        _ = router.route(msg, id: id, topic: "fruit", from: nil, now: .now)

        _ = router.heartbeat(now: .now)
        #expect(router.messageCache.contains(id))
        _ = router.heartbeat(now: .now)
        #expect(router.messageCache.contains(id) == false)
    }

    // MARK: - Seen Cache

    /// Seen message IDs expire `ttl` after they were first seen (re-seeing a message doesn't extend its lifetime)
    @Test func testSeenCacheExpiry() {
        var seen = SeenCache(ttl: .seconds(120))
        let start = ContinuousClock.now
        let id = Data("id-1".utf8)

        let inserted = seen.insert(id, now: start)
        #expect(inserted)
        let insertedAgain = seen.insert(id, now: start + .seconds(60))
        #expect(insertedAgain == false)

        seen.prune(now: start + .seconds(119))
        #expect(seen.contains(id))

        seen.prune(now: start + .seconds(120))
        #expect(seen.contains(id) == false)
        #expect(seen.count == 0)
    }

    // MARK: - Routers

    /// Disconnected peers must be removed from our mesh & topic membership, otherwise they inflate our mesh degree
    @Test func testGossipsubRouterDisconnectCleansMesh() throws {
        var router = GossipSubRouter()
        let meshPeer = try PeerID(.Ed25519)
        let otherPeer = try PeerID(.Ed25519)

        router.handleSubscription(from: meshPeer, topic: "fruit", subscribed: true)
        router.handleSubscription(from: meshPeer, topic: "news", subscribed: true)
        router.handleSubscription(from: otherPeer, topic: "news", subscribed: true)

        /// JOIN grafts the known topic peers
        let outbox = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"] == [meshPeer])
        #expect(outbox.rpcs[meshPeer]?.control.graft.map(\.topicID) == ["fruit"])

        /// A duplicate subscription announcement doesn't change anything
        router.handleSubscription(from: meshPeer, topic: "fruit", subscribed: true)
        #expect(router.mesh["fruit"] == [meshPeer])

        router.removePeer(meshPeer)
        #expect(router.mesh["fruit"]?.isEmpty == true)
        #expect(router.peers(subscribedTo: "fruit").isEmpty)
        #expect(router.peers(subscribedTo: "news") == [otherPeer])
    }

    /// Floodsub floods to every known topic peer (even for topics we're not subscribed to) and forgets disconnected peers
    @Test func testFloodsubRouterFloodTargetsAndDisconnect() throws {
        var router = FloodSubRouter()
        let peer = try PeerID(.Ed25519)
        router.handleSubscription(from: peer, topic: "news", subscribed: true)

        #expect(router.route(Self.makeMessage(data: "banana"), id: Data(), topic: "news", from: nil, now: .now) == [peer])

        router.removePeer(peer)
        #expect(router.route(Self.makeMessage(data: "banana"), id: Data(), topic: "news", from: nil, now: .now).isEmpty)
    }

    // MARK: - Signing & Signature Policies

    /// Ed25519 PeerIDs inline their public key, so (like go / rust) we omit the `key` field and the receiver extracts the key from `from`
    @Test func testStrictSignWithInlinedKey() throws {
        let author = try PeerID(.Ed25519)
        let msg = Self.makeMessage(data: "banana", from: Data(author.id), seqno: Self.seqno(1))
        let signed = try MessageSigning.prepare(msg, policy: .strictSign, signer: author)

        #expect(!signed.signature.isEmpty)
        #expect(signed.hasKey == false, "Ed25519 keys are inlined in the PeerID, the key field should be omitted")
        #expect(MessageSigning.check(signed, against: .strictSign) == nil)

        /// Tampering with the payload invalidates the signature
        var tampered = signed
        tampered.data = Data("pineapple".utf8)
        #expect(MessageSigning.check(tampered, against: .strictSign) == .invalidSignature)

        /// An unsigned message is rejected under StrictSign
        #expect(MessageSigning.check(msg, against: .strictSign) == .missingSignature)
    }

    /// When the `key` field is present it must belong to the `from` PeerID
    @Test func testStrictSignRejectsMismatchedKey() throws {
        let author = try PeerID(.Ed25519)
        let msg = Self.makeMessage(data: "banana", from: Data(author.id), seqno: Self.seqno(1))

        /// Including the author's (matching) key is allowed
        var withKey = try MessageSigning.prepare(msg, policy: .strictSign, signer: author)
        withKey.key = try Data(author.marshalPublicKey())
        #expect(MessageSigning.check(withKey, against: .strictSign) == nil)

        /// Someone else's key is not
        var wrongKey = withKey
        wrongKey.key = try Data(PeerID(.Ed25519).marshalPublicKey())
        #expect(MessageSigning.check(wrongKey, against: .strictSign) == .invalidSignature)
    }

    /// StrictNoSign messages must omit (and receivers must reject) the `from`, `seqno`, `signature` and `key` fields
    @Test func testStrictNoSign() throws {
        let author = try PeerID(.Ed25519)
        let msg = Self.makeMessage(data: "banana", from: Data(author.id), seqno: Self.seqno(1))
        let prepared = try MessageSigning.prepare(msg, policy: .strictNoSign, signer: author)
        #expect(!prepared.hasFrom && !prepared.hasSeqno && !prepared.hasSignature && !prepared.hasKey)
        #expect(MessageSigning.check(prepared, against: .strictNoSign) == nil)

        /// Messages containing authorship info are rejected
        #expect(MessageSigning.check(msg, against: .strictNoSign) == .unexpectedAuthorship)

        /// The from+seqno ID strategies would collide under StrictNoSign, so a content based ID is substituted
        let config = TopicConfiguration(
            .init(
                topic: "fruit",
                signaturePolicy: .strictNoSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        let other = try MessageSigning.prepare(
            Self.makeMessage(data: "pineapple"),
            policy: .strictNoSign,
            signer: author
        )
        let idStrategy = config.effectiveMessageID
        #expect(!idStrategy.id(for: prepared).isEmpty)
        #expect(idStrategy.id(for: prepared) != idStrategy.id(for: other))
    }

    /// The core `Hasher` based message ID functions differ between processes, so they're mapped to stable SHA-256 IDs
    @Test func testLegacyMessageIDFunctionsAreStable() throws {
        let msg = Self.makeMessage(data: "banana", from: Data("author".utf8), seqno: Self.seqno(7))
        for function in [PubSub.MessageIDFunction.hashSequenceNumberAndFromFields, .hashEverything] {
            let config = TopicConfiguration(
                .init(topic: "fruit", signaturePolicy: .strictSign, validator: .acceptAll, messageIDFunc: function)
            )
            let id = config.effectiveMessageID.id(for: msg)
            #expect(id.count == 32)
            #expect(id == config.effectiveMessageID.id(for: msg))
        }
    }

    /// A message claiming multiple topics could bypass a topic's policy / validators, so it's rejected outright
    @Test func testMultiTopicMessagesAreRejected() {
        #expect(
            MessageSigning.check(Self.makeMessage(data: "banana", topics: ["fruit"]), against: .strictNoSign) == nil
        )
        #expect(
            MessageSigning.check(Self.makeMessage(data: "banana", topics: ["fruit", "victim"]), against: .strictNoSign)
                == .invalidTopicCount(2)
        )
        #expect(
            MessageSigning.check(Self.makeMessage(data: "banana", topics: []), against: .strictNoSign)
                == .invalidTopicCount(0)
        )
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
            .init(
                topic: "fruit",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        #expect(try await gsub.getTopics().contains("fruit"))

        try await gsub.unsubscribe(topic: "fruit", on: nil).get()
        #expect(try await gsub.getTopics().contains("fruit") == false)
        let hasMesh = await gsub.engine.inspectRouter { router in
            (router as? GossipSubRouter)?.mesh["fruit"] != nil
        }
        #expect(hasMesh == false)

        /// Resubscribing works
        let _: PubSub.SubscriptionHandler = try gsub.subscribe(
            .init(
                topic: "fruit",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
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
    func asyncMap<T>(_ transform: (Element) async throws -> T) async rethrows -> [T] {
        var results: [T] = []
        for element in self { results.append(try await transform(element)) }
        return results
    }
}
