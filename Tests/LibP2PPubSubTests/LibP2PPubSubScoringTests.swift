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

/// Deterministic tests of GossipSub v1.1 peer scoring
@Suite("Libp2p PubSub Scoring Tests")
struct LibP2PPubSubScoringTests {

    // MARK: - Helpers

    private static func peers(_ count: Int) throws -> [PeerID] {
        try (0..<count).map { _ in try PeerID(.Ed25519) }
    }

    private static func message(_ data: String, topic: String = "fruit") -> RPC.Message {
        RPC.Message.with {
            $0.data = Data(data.utf8)
            $0.topicIds = [topic]
        }
    }

    /// Topic parameters with every component disabled, so tests can enable just the one they're exercising
    private static func silentTopic() -> TopicScoreParameters {
        TopicScoreParameters(timeInMeshWeight: 0, firstMessageDeliveriesWeight: 0, invalidMessageDeliveriesWeight: 0)
    }

    /// Score parameters for the "fruit" topic, without the default behavioural penalty
    private static func scoreParameters(
        _ topic: TopicScoreParameters,
        configure: (inout PeerScoreParameters) -> Void = { _ in }
    ) -> PeerScoreParameters {
        var parameters = PeerScoreParameters(topics: ["fruit": topic], behaviourPenaltyWeight: 0)
        configure(&parameters)
        return parameters
    }

    /// A scoring GossipSub router with `peers` subscribed to "fruit"
    private static func scoringRouter(
        peers: [PeerID],
        parameters: PeerScoreParameters,
        thresholds: PeerScoreThresholds = .init(),
        configure: (inout GossipSubParameters) -> Void = { _ in }
    ) throws -> GossipSubRouter {
        var gossipParameters = GossipSubParameters(
            floodPublish: false,
            scoring: try GossipSubScoring(parameters: parameters, thresholds: thresholds)
        )
        configure(&gossipParameters)
        var router = GossipSubRouter(parameters: gossipParameters)
        for peer in peers {
            router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: false, ip: nil)
            router.handleSubscription(from: peer, topic: "fruit", subscribed: true)
        }
        return router
    }

    /// Makes `peer` deliver an invalid message on "fruit" (a P4 penalty)
    private static func deliverInvalidMessage(_ router: inout GossipSubRouter, from peer: PeerID, id: String) {
        let message = Self.message(id)
        _ = router.received(message, id: Data(id.utf8), topic: "fruit", from: peer, now: .now)
        router.rejected(message, id: Data(id.utf8), topic: "fruit", from: peer, reason: .invalid, now: .now)
    }

    // MARK: - Score components

    /// P1 rewards time in the mesh, in whole quanta, up to the cap
    @Test func testTimeInMesh() throws {
        var topic = Self.silentTopic()
        topic.timeInMeshWeight = 1
        topic.timeInMeshCap = 10
        var score = PeerScore(parameters: Self.scoreParameters(topic))
        let peer = try PeerID(.Ed25519)
        let start = ContinuousClock.now

        score.addPeer(peer, ip: nil)
        score.graft(peer, topic: "fruit", now: start)
        score.refresh(now: start + .milliseconds(3500))
        #expect(score.score(of: peer) == 3)

        score.refresh(now: start + .seconds(60))
        #expect(score.score(of: peer) == 10)

        score.prune(peer, topic: "fruit")
        #expect(score.score(of: peer) == 0)
    }

    /// P2 rewards first deliveries, and decays
    @Test func testFirstMessageDeliveries() throws {
        var topic = Self.silentTopic()
        topic.firstMessageDeliveriesWeight = 1
        topic.firstMessageDeliveriesDecay = 0.5
        var score = PeerScore(parameters: Self.scoreParameters(topic))
        let peer = try PeerID(.Ed25519)
        score.addPeer(peer, ip: nil)

        for id in ["a", "b", "c"] {
            score.validationStarted(Data(id.utf8), topic: "fruit", now: .now)
            score.delivered(Data(id.utf8), topic: "fruit", from: peer, now: .now)
        }
        #expect(score.score(of: peer) == 3)

        score.refresh(now: .now)
        #expect(score.score(of: peer) == 1.5)
    }

    /// P3 penalizes an active mesh peer's delivery deficit, and P3b keeps that deficit as a sticky penalty once it's pruned
    @Test func testMeshMessageDeliveriesAndFailurePenalty() throws {
        var topic = Self.silentTopic()
        topic.meshMessageDeliveriesWeight = -1
        topic.meshMessageDeliveriesThreshold = 4
        topic.meshMessageDeliveriesActivation = .seconds(1)
        topic.meshFailurePenaltyWeight = -1
        var score = PeerScore(parameters: Self.scoreParameters(topic))
        let peer = try PeerID(.Ed25519)
        let start = ContinuousClock.now
        score.addPeer(peer, ip: nil)
        score.graft(peer, topic: "fruit", now: start)

        /// Not active yet
        score.refresh(now: start)
        #expect(score.score(of: peer) == 0)

        /// Active after `meshMessageDeliveriesActivation`, one delivery leaves a deficit of 3
        score.refresh(now: start + .seconds(2))
        score.validationStarted(Data("a".utf8), topic: "fruit", now: start + .seconds(2))
        score.delivered(Data("a".utf8), topic: "fruit", from: peer, now: start + .seconds(2))
        #expect(score.score(of: peer) == -9)

        /// Pruned with that deficit, it becomes a sticky P3b penalty
        score.prune(peer, topic: "fruit")
        #expect(score.score(of: peer) == -9)
        #expect(score.peers[peer]?.topics["fruit"]?.meshFailurePenalty == 9)
        #expect(score.peers[peer]?.topics["fruit"]?.meshMessageDeliveriesActive == false)
    }

    /// Mesh peers get credit for delivering a message during its validation, or within the delivery window after it
    @Test func testNearFirstMeshDeliveries() throws {
        var topic = Self.silentTopic()
        topic.meshMessageDeliveriesWeight = -1
        topic.meshMessageDeliveriesWindow = .milliseconds(10)
        var score = PeerScore(parameters: Self.scoreParameters(topic))
        let (source, during, late) = try (PeerID(.Ed25519), PeerID(.Ed25519), PeerID(.Ed25519))
        let start = ContinuousClock.now
        for peer in [source, during, late] {
            score.addPeer(peer, ip: nil)
            score.graft(peer, topic: "fruit", now: start)
        }
        let id = Data("a".utf8)

        score.validationStarted(id, topic: "fruit", now: start)
        score.duplicate(id, topic: "fruit", from: during, now: start)
        score.delivered(id, topic: "fruit", from: source, now: start)
        score.duplicate(id, topic: "fruit", from: late, now: start + .milliseconds(50))

        let deliveries = { (peer: PeerID) in score.peers[peer]?.topics["fruit"]?.meshMessageDeliveries }
        #expect(deliveries(source) == 1)
        #expect(deliveries(during) == 1)
        #expect(deliveries(late) == 0)
    }

    /// P4 penalizes invalid messages: the source and every peer that delivered it, by the square of the count
    @Test func testInvalidMessageDeliveries() throws {
        var topic = Self.silentTopic()
        topic.invalidMessageDeliveriesWeight = -1
        var score = PeerScore(parameters: Self.scoreParameters(topic))
        let (source, copier, bystander, forger) = try (
            PeerID(.Ed25519), PeerID(.Ed25519), PeerID(.Ed25519), PeerID(.Ed25519)
        )

        for (index, id) in ["a", "b"].enumerated() {
            let id = Data(id.utf8)
            score.validationStarted(id, topic: "fruit", now: .now)
            if index == 0 { score.duplicate(id, topic: "fruit", from: copier, now: .now) }
            score.rejected(id, topic: "fruit", from: source, reason: .invalid, now: .now)
        }
        #expect(score.score(of: source) == -4)
        #expect(score.score(of: copier) == -1)

        /// A copy of an invalid message arriving later is penalized too
        score.duplicate(Data("a".utf8), topic: "fruit", from: bystander, now: .now)
        #expect(score.score(of: bystander) == -1)

        /// Ignored messages aren't penalized, bad signatures are
        score.validationStarted(Data("c".utf8), topic: "fruit", now: .now)
        score.rejected(Data("c".utf8), topic: "fruit", from: bystander, reason: .ignored, now: .now)
        #expect(score.score(of: bystander) == -1)
        score.rejected(Data("d".utf8), topic: "fruit", from: forger, reason: .invalidSignature, now: .now)
        #expect(score.score(of: forger) == -1)
    }

    /// P5 is the application's score, P7 the (squared) behavioural penalty above its threshold, and topics can be capped
    @Test func testApplicationScoreBehaviourPenaltyAndCap() throws {
        let (favourite, misbehaving, prolific) = try (PeerID(.Ed25519), PeerID(.Ed25519), PeerID(.Ed25519))
        var topic = Self.silentTopic()
        topic.firstMessageDeliveriesWeight = 1
        let parameters = Self.scoreParameters(topic) {
            $0.appSpecificScore = { $0 == favourite ? 5 : 0 }
            $0.appSpecificWeight = 2
            $0.behaviourPenaltyWeight = -1
            $0.behaviourPenaltyThreshold = 1
            $0.behaviourPenaltyDecay = 0.5
            $0.topicScoreCap = 5
        }
        var score = PeerScore(parameters: parameters)

        /// Like go-libp2p-pubsub, peers we have no stats for (ex: never connected) score 0
        #expect(score.score(of: favourite) == 0)
        score.addPeer(favourite, ip: nil)
        #expect(score.score(of: favourite) == 10)

        score.addPenalty(misbehaving, count: 3)
        #expect(score.score(of: misbehaving) == -4)
        score.refresh(now: .now)
        #expect(score.score(of: misbehaving) == -0.25)

        for id in 0..<10 {
            score.validationStarted(Data([UInt8(id)]), topic: "fruit", now: .now)
            score.delivered(Data([UInt8(id)]), topic: "fruit", from: prolific, now: .now)
        }
        #expect(score.score(of: prolific) == 5, "Topic contributions are capped")
    }

    /// P6 penalizes the square of the number of peers above the threshold sharing an IP (unless it's whitelisted)
    @Test func testIPColocation() throws {
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.ipColocationFactorWeight = -1
            $0.ipColocationFactorThreshold = 1
            $0.ipColocationFactorWhitelist = ["10.0.0.1"]
        }
        var score = PeerScore(parameters: parameters)
        let colocated = try Self.peers(3)
        let whitelisted = try Self.peers(3)
        for peer in colocated { score.addPeer(peer, ip: "1.2.3.4") }
        for peer in whitelisted { score.addPeer(peer, ip: "10.0.0.1") }

        #expect(colocated.allSatisfy { score.score(of: $0) == -4 })
        #expect(whitelisted.allSatisfy { score.score(of: $0) == 0 })

        /// Disconnected peers no longer count
        score.removePeer(colocated[0], now: .now)
        #expect(score.score(of: colocated[1]) == -1)
    }

    /// Peers that disconnect with a non-positive score are remembered for `retainScore`, positive scores are forgotten
    @Test func testScoreRetention() throws {
        let (bad, good) = try (PeerID(.Ed25519), PeerID(.Ed25519))
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.appSpecificScore = { $0 == good ? 1 : 0 }
            $0.behaviourPenaltyWeight = -1
            $0.retainScore = .seconds(10)
        }
        var score = PeerScore(parameters: parameters)
        let start = ContinuousClock.now
        score.addPeer(bad, ip: nil)
        score.addPeer(good, ip: nil)
        score.addPenalty(bad, count: 2)

        score.removePeer(bad, now: start)
        score.removePeer(good, now: start)
        #expect(score.peers[good] == nil)
        #expect(score.peers[bad]?.connected == false)

        /// Reconnecting doesn't reset a bad score
        score.addPeer(bad, ip: nil)
        #expect(score.score(of: bad) < 0)

        /// Once `retainScore` elapses after disconnecting, it's forgotten
        score.removePeer(bad, now: start)
        score.refresh(now: start + .seconds(11))
        #expect(score.peers[bad] == nil)
    }

    // MARK: - Parameter validation

    @Test func testParameterValidation() throws {
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(parameters: .init(), thresholds: .init(gossipThreshold: 1))
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(parameters: .init(), thresholds: .init(gossipThreshold: -10, publishThreshold: -5))
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(parameters: .init(), thresholds: .init(acceptPXThreshold: -1))
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(parameters: .init(behaviourPenaltyWeight: 1))
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(parameters: .init(decayInterval: .milliseconds(100)))
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(
                parameters: .init(topics: ["fruit": TopicScoreParameters(invalidMessageDeliveriesWeight: 1)])
            )
        }
        #expect(throws: PeerScoreParameterError.self) {
            try GossipSubScoring(
                parameters: .init(topics: [
                    "fruit": TopicScoreParameters(meshMessageDeliveriesWeight: -1, meshMessageDeliveriesThreshold: 0)
                ])
            )
        }
        _ = try GossipSubScoring(parameters: .init(topics: ["fruit": TopicScoreParameters()]))
    }

    // MARK: - Router behaviour

    /// Negative scoring peers are pruned from our meshes (without PX), and not grafted again
    @Test func testNegativeScorePeersArePruned() throws {
        let peers = try Self.peers(6)
        var topic = Self.silentTopic()
        topic.invalidMessageDeliveriesWeight = -10
        var router = try Self.scoringRouter(peers: peers, parameters: Self.scoreParameters(topic)) {
            $0.peerExchange = true
        }
        _ = router.join("fruit", now: .now)
        let offender = peers[0]
        Self.deliverInvalidMessage(&router, from: offender, id: "bad")
        #expect(router.score(of: offender) < 0)

        let outbox = router.heartbeat(now: .now)
        #expect(router.mesh["fruit"]?.contains(offender) == false)
        #expect(outbox.rpcs[offender]?.control.prune.first?.peers.isEmpty == true)
        _ = router.heartbeat(now: .now)
        #expect(router.mesh["fruit"]?.contains(offender) == false)

        /// Its GRAFTs are rejected too
        let graft = router.handleControl(
            .with { $0.graft = [.with { $0.topicID = "fruit" }] },
            from: offender,
            hasSeen: { _ in false },
            now: .now
        )
        #expect(graft.rpcs[offender]?.control.prune.isEmpty == false)
        #expect(router.mesh["fruit"]?.contains(offender) == false)
    }

    /// Peers below our graylist threshold are ignored entirely
    @Test func testGraylisting() throws {
        let peer = try PeerID(.Ed25519)
        var topic = Self.silentTopic()
        topic.invalidMessageDeliveriesWeight = -100
        var router = try Self.scoringRouter(peers: [peer], parameters: Self.scoreParameters(topic))
        #expect(router.accepts(rpcFrom: peer))
        Self.deliverInvalidMessage(&router, from: peer, id: "bad")
        #expect(router.accepts(rpcFrom: peer) == false)
    }

    /// Peers below our gossip threshold aren't gossiped to, and their gossip is ignored.
    /// Peers below our publish threshold aren't flood published to.
    @Test func testGossipAndPublishThresholds() throws {
        let peers = try Self.peers(10)
        var topic = Self.silentTopic()
        topic.invalidMessageDeliveriesWeight = -1
        /// Decay slowly, so the heartbeat below doesn't lift the peers back above the thresholds
        topic.invalidMessageDeliveriesDecay = 0.99
        var router = try Self.scoringRouter(peers: peers, parameters: Self.scoreParameters(topic))
        _ = router.join("fruit", now: .now)
        let mesh = try #require(router.mesh["fruit"])
        let outsiders = Array(Set(peers).subtracting(mesh))
        let (gossipless, unpublishable) = (outsiders[0], outsiders[1])
        /// -25 is below our gossip threshold (-10), -64 is also below our publish threshold (-50)
        for index in 0..<5 { Self.deliverInvalidMessage(&router, from: gossipless, id: "g\(index)") }
        for index in 0..<8 { Self.deliverInvalidMessage(&router, from: unpublishable, id: "p\(index)") }
        #expect(router.score(of: gossipless) < -10 && router.score(of: gossipless) > -50)
        #expect(router.score(of: unpublishable) < -50)

        /// Their IHAVEs are ignored
        let iHave = RPC.ControlMessage.with {
            $0.ihave = [
                .with {
                    $0.topicID = "fruit"
                    $0.messageIds = [Data("x".utf8)]
                }
            ]
        }
        #expect(router.handleControl(iHave, from: gossipless, hasSeen: { _ in false }, now: .now).isEmpty)

        /// They aren't gossiped to
        _ = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)
        let gossiped = Set(router.heartbeat(now: .now).rpcs.filter { !$0.value.control.ihave.isEmpty }.keys)
        #expect(!gossiped.contains(gossipless) && !gossiped.contains(unpublishable))

        /// Flood publishing skips peers below the publish threshold only
        var flooding = try Self.scoringRouter(peers: peers, parameters: Self.scoreParameters(topic)) {
            $0.floodPublish = true
        }
        for index in 0..<5 { Self.deliverInvalidMessage(&flooding, from: gossipless, id: "g\(index)") }
        for index in 0..<8 { Self.deliverInvalidMessage(&flooding, from: unpublishable, id: "p\(index)") }
        let targets = flooding.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: nil, now: .now)
        #expect(targets.contains(gossipless))
        #expect(!targets.contains(unpublishable))
        #expect(targets.count == peers.count - 1)
    }

    /// Only peers scoring at least our accept PX threshold may suggest peers to us
    @Test func testAcceptPXThreshold() throws {
        let (trusted, stranger) = try (PeerID(.Ed25519), PeerID(.Ed25519))
        let suggested = try PeerID(.Ed25519)
        let parameters = Self.scoreParameters(Self.silentTopic()) { $0.appSpecificScore = { $0 == trusted ? 20 : 0 } }
        var router = try Self.scoringRouter(peers: [trusted, stranger], parameters: parameters) {
            $0.peerExchange = true
        }
        _ = router.join("fruit", now: .now)

        let prune = RPC.ControlMessage.with {
            $0.prune = [
                .with { prune in
                    prune.topicID = "fruit"
                    prune.peers = [.with { $0.peerID = Data(suggested.id) }]
                }
            ]
        }
        #expect(router.handleControl(prune, from: stranger, hasSeen: { _ in false }, now: .now).dials.isEmpty)
        #expect(router.handleControl(prune, from: trusted, hasSeen: { _ in false }, now: .now).dials == [suggested])
    }

    /// Grafting while backed off is penalized, twice when it's within `graftFloodThreshold` of the prune
    @Test func testGraftFloodPenalty() throws {
        let peer = try PeerID(.Ed25519)
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.behaviourPenaltyWeight = -1
            $0.behaviourPenaltyThreshold = 0
        }
        var router = try Self.scoringRouter(peers: [peer], parameters: parameters)
        let start = ContinuousClock.now
        _ = router.join("fruit", now: start)
        let graft = RPC.ControlMessage.with { $0.graft = [.with { $0.topicID = "fruit" }] }

        /// The peer prunes us, then immediately grafts us again
        _ = router.handleControl(
            .with { $0.prune = [.with { $0.topicID = "fruit" }] },
            from: peer,
            hasSeen: { _ in false },
            now: start
        )
        _ = router.handleControl(graft, from: peer, hasSeen: { _ in false }, now: start + .seconds(1))
        #expect(router.score(of: peer) == -4)

        /// Grafting again (still backed off, but outside the flood threshold) is penalized once
        _ = router.handleControl(graft, from: peer, hasSeen: { _ in false }, now: start + .seconds(30))
        #expect(router.score(of: peer) == -9)
    }

    /// A peer that doesn't deliver a message it advertised (within `iWantFollowupTime` of our IWANT) breaks its promise
    @Test func testBrokenPromises() throws {
        let (flaky, reliable) = try (PeerID(.Ed25519), PeerID(.Ed25519))
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.behaviourPenaltyWeight = -1
            $0.behaviourPenaltyThreshold = 0
        }
        var router = try Self.scoringRouter(peers: [flaky, reliable], parameters: parameters)
        let start = ContinuousClock.now
        _ = router.join("fruit", now: start)
        func iHave(_ id: String) -> RPC.ControlMessage {
            .with {
                $0.ihave = [
                    .with {
                        $0.topicID = "fruit"
                        $0.messageIds = [Data(id.utf8)]
                    }
                ]
            }
        }

        #expect(
            router.handleControl(iHave("a"), from: flaky, hasSeen: { _ in false }, now: start).rpcs[flaky]?.control
                .iwant.isEmpty == false
        )
        #expect(
            router.handleControl(iHave("b"), from: reliable, hasSeen: { _ in false }, now: start).rpcs[reliable]?
                .control.iwant.isEmpty == false
        )
        /// Only the reliable peer's message arrives
        _ = router.received(
            Self.message("b"),
            id: Data("b".utf8),
            topic: "fruit",
            from: reliable,
            now: start + .seconds(1)
        )

        _ = router.heartbeat(now: start + .seconds(4))
        #expect(router.score(of: flaky) == -1)
        #expect(router.score(of: reliable) == 0)
        #expect(router.promises.isEmpty)
    }

    /// Pruning an oversubscribed mesh keeps the `D_score` highest scoring peers
    @Test func testScoreRetentionWhenPruning() throws {
        let peers = try Self.peers(15)
        let stars = Set(peers.prefix(4))
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.appSpecificScore = { stars.contains($0) ? 100 : 0 }
        }
        var router = try Self.scoringRouter(peers: peers, parameters: parameters)
        for peer in peers { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true, ip: nil) }
        _ = router.join("fruit", now: .now)
        for peer in peers {
            _ = router.handleControl(
                .with { $0.graft = [.with { $0.topicID = "fruit" }] },
                from: peer,
                hasSeen: { _ in false },
                now: .now
            )
        }
        #expect(router.mesh["fruit"]?.count == 15)

        for _ in 0..<5 {
            var trial = router
            _ = trial.heartbeat(now: .now)
            let mesh = try #require(trial.mesh["fruit"])
            #expect(mesh.count == 6)
            #expect(mesh.isSuperset(of: stars))
        }
    }

    /// When a mesh's median score drops below the opportunistic graft threshold, we graft peers scoring above the median
    @Test func testOpportunisticGrafting() throws {
        let ordinary = try Self.peers(6)
        let stars = try Self.peers(3)
        let parameters = Self.scoreParameters(Self.silentTopic()) {
            $0.appSpecificScore = { stars.contains($0) ? 10 : 0 }
        }
        var router = try Self.scoringRouter(
            peers: ordinary,
            parameters: parameters,
            thresholds: .init(opportunisticGraftThreshold: 5)
        ) { $0.opportunisticGraftTicks = 1 }
        _ = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"] == Set(ordinary))

        for peer in stars {
            router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: false, ip: nil)
            router.handleSubscription(from: peer, topic: "fruit", subscribed: true)
        }
        let outbox = router.heartbeat(now: .now)
        let mesh = try #require(router.mesh["fruit"])
        #expect(mesh.count == 8)
        #expect(mesh.intersection(stars).count == 2)
        #expect(Set(outbox.rpcs.filter { !$0.value.control.graft.isEmpty }.keys).isSubset(of: Set(stars)))
    }

    // MARK: - Engine

    /// Copies of a message delivered while it's being validated aren't validated again, and are scored with the original.
    /// Here the message is invalid, so both its source and the peer that delivered the copy are penalized.
    @Test func testDuplicatesDuringValidation() async throws {
        var topic = Self.silentTopic()
        topic.invalidMessageDeliveriesWeight = -1
        let scoring = try GossipSubScoring(parameters: Self.scoreParameters(topic))
        var logger = Logger(label: "scoring-tests")
        logger.logLevel = .critical
        let engine = PubSubEngine(
            protocolIDs: [GossipSub.multicodec],
            localPeer: try PeerID(.Ed25519),
            configuration: .init(),
            router: GossipSubRouter(parameters: .init(scoring: scoring)),
            logger: logger
        )
        await engine.start()

        let validations = ValidationCounter()
        let subscription = try await engine.subscribe(
            TopicConfiguration(
                topic: "fruit",
                validator: MessageValidator { _, _ in
                    validations.increment()
                    try? await Task.sleep(for: .milliseconds(200))
                    return .reject
                }
            )
        )

        let author = try PeerID(.Ed25519)
        let copier = try PeerID(.Ed25519)
        let message = try MessageSigning.prepare(
            RPC.Message.with {
                $0.data = Data("bad".utf8)
                $0.topicIds = ["fruit"]
                $0.from = Data(author.id)
                $0.seqno = Data([0, 0, 0, 0, 0, 0, 0, 1])
            },
            policy: .strictSign,
            signer: author
        )
        let frame = ByteBuffer(bytes: try RPC.with { $0.msgs = [message] }.serializedData())

        async let first: Void = engine.handle(frame, from: author)
        try await Task.sleep(for: .milliseconds(50))
        await engine.handle(frame, from: copier)
        await first

        #expect(validations.count == 1)
        let scores = await engine.inspectRouter { router in
            [author, copier].map { (router as? GossipSubRouter)?.score(of: $0) ?? 0 }
        }
        #expect(scores == [-1, -1])

        subscription.cancel()
        await engine.stop()
    }
}

/// A thread safe counter of validator invocations
private final class ValidationCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _count = 0
    var count: Int { lock.withLock { _count } }
    func increment() { lock.withLock { _count += 1 } }
}
