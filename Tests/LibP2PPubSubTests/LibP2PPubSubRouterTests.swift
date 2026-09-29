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

/// Deterministic tests of the routing state machines
@Suite("Libp2p PubSub Router Tests")
struct LibP2PPubSubRouterTests {

    private static func peers(_ count: Int) throws -> [PeerID] {
        try (0..<count).map { _ in try PeerID(.Ed25519) }
    }

    private static func message(_ data: String, topic: String = "fruit") -> RPC.Message {
        RPC.Message.with {
            $0.data = Data(data.utf8)
            $0.topicIds = [topic]
        }
    }

    /// A router with `count` peers subscribed to `topic`
    private static func gossipRouter(
        peers: [PeerID],
        topic: String = "fruit",
        parameters: GossipSubParameters = .init()
    ) -> GossipSubRouter {
        var router = GossipSubRouter(parameters: parameters)
        for peer in peers { router.handleSubscription(from: peer, topic: topic, subscribed: true) }
        return router
    }

    // MARK: - JOIN / LEAVE

    @Test func testJoinGraftsUpToDPeers() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)

        let outbox = router.join("fruit")
        let mesh = try #require(router.mesh["fruit"])
        #expect(mesh.count == 6)
        #expect(mesh.isSubset(of: Set(peers)))
        /// Every mesh peer is sent a GRAFT, and nobody else is
        #expect(Set(outbox.rpcs.keys) == mesh)
        #expect(outbox.rpcs.values.allSatisfy { $0.control.graft.map(\.topicID) == ["fruit"] })
    }

    @Test func testLeavePrunesTheMesh() throws {
        var router = Self.gossipRouter(peers: try Self.peers(3))
        _ = router.join("fruit")
        let mesh = try #require(router.mesh["fruit"])

        let outbox = router.leave("fruit")
        #expect(router.mesh["fruit"] == nil)
        #expect(Set(outbox.rpcs.keys) == mesh)
        #expect(outbox.rpcs.values.allSatisfy { $0.control.prune.map(\.topicID) == ["fruit"] })
    }

    // MARK: - Heartbeat mesh maintenance

    @Test func testHeartbeatGraftsWhenBelowDLow() throws {
        var router = GossipSubRouter()
        _ = router.join("fruit")
        #expect(router.mesh["fruit"]?.isEmpty == true)

        /// Peers subscribe after we joined, the heartbeat grafts them (up to D)
        for peer in try Self.peers(8) { router.handleSubscription(from: peer, topic: "fruit", subscribed: true) }
        let outbox = router.heartbeat(now: .now)

        #expect(router.mesh["fruit"]?.count == 6)
        #expect(outbox.rpcs.values.filter { !$0.control.graft.isEmpty }.count == 6)
    }

    @Test func testHeartbeatPrunesWhenAboveDHigh() throws {
        let peers = try Self.peers(15)
        var router = Self.gossipRouter(peers: peers)
        _ = router.join("fruit")

        /// Peers graft themselves onto our mesh until we're over D_hi
        for peer in peers {
            _ = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false })
        }
        #expect(router.mesh["fruit"]?.count == 15)

        let outbox = router.heartbeat(now: .now)
        #expect(router.mesh["fruit"]?.count == 6)
        #expect(outbox.rpcs.values.filter { !$0.control.prune.isEmpty }.count == 9)
    }

    // MARK: - Control messages

    @Test func testGraft() throws {
        let peer = try PeerID(.Ed25519)
        var router = GossipSubRouter()
        _ = router.join("fruit")

        /// Grafting onto a topic we're subscribed to adds the peer to our mesh, with no response
        let accepted = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false })
        #expect(router.mesh["fruit"] == [peer])
        #expect(accepted.isEmpty)

        /// Grafting onto a topic we're not subscribed to is answered with a PRUNE
        let rejected = router.handleControl(.with { $0.graft = [.with { $0.topicID = "news" }] }, from: peer, hasSeen: { _ in false })
        #expect(router.mesh["news"] == nil)
        #expect(rejected.rpcs[peer]?.control.prune.map(\.topicID) == ["news"])
    }

    @Test func testPrune() throws {
        let peer = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: [peer])
        _ = router.join("fruit")
        #expect(router.mesh["fruit"] == [peer])

        let outbox = router.handleControl(.with { $0.prune = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false })
        #expect(router.mesh["fruit"]?.isEmpty == true)
        #expect(outbox.isEmpty)
    }

    @Test func testIHaveRequestsUnseenMessages() throws {
        let peer = try PeerID(.Ed25519)
        var router = GossipSubRouter()
        _ = router.join("fruit")
        let seen = Data("seen".utf8)
        let unseen = Data("unseen".utf8)

        let outbox = router.handleControl(
            .with {
                $0.ihave = [
                    .with { $0.topicID = "fruit"; $0.messageIds = [seen, unseen, unseen] },
                    /// IHAVEs for topics we're not subscribed to are ignored
                    .with { $0.topicID = "news"; $0.messageIds = [Data("news".utf8)] },
                ]
            },
            from: peer,
            hasSeen: { $0 == seen }
        )
        #expect(outbox.rpcs[peer]?.control.iwant.flatMap(\.messageIds) == [unseen])
    }

    @Test func testIWantRespondsWithCachedMessages() throws {
        let peer = try PeerID(.Ed25519)
        var router = GossipSubRouter()
        let id = Data("banana".utf8)
        _ = router.route(Self.message("banana"), id: id, topic: "fruit", from: nil, now: .now)

        let outbox = router.handleControl(
            .with { $0.iwant = [.with { $0.messageIds = [id, Data("unknown".utf8), id] }] },
            from: peer,
            hasSeen: { _ in true }
        )
        #expect(outbox.rpcs[peer]?.msgs.map { String(decoding: $0.data, as: UTF8.self) } == ["banana"])
    }

    // MARK: - Routing & gossip

    @Test func testRouting() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)

        /// Before joining, our own messages go to the topic's fanout (`D` random topic peers)
        let fanoutTargets = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)
        #expect(fanoutTargets.count == 6)
        #expect(fanoutTargets.isSubset(of: Set(peers)))
        #expect(router.fanout["fruit"] == fanoutTargets)

        /// Once joined, messages go to our mesh
        _ = router.join("fruit")
        let mesh = try #require(router.mesh["fruit"])
        #expect(router.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: peers[0], now: .now) == mesh)
    }

    // MARK: - Fanout

    /// Publishing to a topic we're not subscribed to keeps using the same fanout peers, until `fanout_ttl` after our last publish
    @Test func testFanoutLifetime() throws {
        let start = ContinuousClock.now
        var router = Self.gossipRouter(peers: try Self.peers(10), parameters: .init(fanoutTTL: .seconds(60)))

        let first = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: start)
        let second = router.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: nil, now: start + .seconds(30))
        #expect(first == second)

        /// Still within `fanout_ttl` of our last publish
        _ = router.heartbeat(now: start + .seconds(89))
        #expect(router.fanout["fruit"] == second)

        /// `fanout_ttl` after our last publish, the fanout is forgotten
        _ = router.heartbeat(now: start + .seconds(90))
        #expect(router.fanout["fruit"] == nil)
        #expect(router.lastPublished["fruit"] == nil)
    }

    /// Fanout peers that unsubscribe (or disconnect) are replaced by the heartbeat
    @Test func testFanoutMaintenance() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)
        let fanout = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)

        let degree = GossipSubParameters().meshDegree
        #expect(degree < peers.count, "mesh degree exceeds number of network peers")
        
        let leaving = try #require(fanout.first)
        router.handleSubscription(from: leaving, topic: "fruit", subscribed: false)
        #expect(router.fanout["fruit"]?.contains(leaving) == false)
        #expect(router.fanout["fruit"]?.count == degree - 1)

        _ = router.heartbeat(now: .now)
        #expect(router.fanout["fruit"]?.count == degree)
        #expect(router.fanout["fruit"]?.contains(leaving) == false)
    }

    /// Joining a topic we've been publishing to grafts our fanout peers first, and forgets the fanout
    @Test func testJoinPromotesFanoutPeers() throws {
        var router = Self.gossipRouter(peers: try Self.peers(10))
        let fanout = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)

        let outbox = router.join("fruit")
        #expect(router.mesh["fruit"] == fanout)
        #expect(Set(outbox.rpcs.keys) == fanout)
        #expect(router.fanout["fruit"] == nil)
        #expect(router.lastPublished["fruit"] == nil)
    }

    /// While our mesh is empty (ex: before our first heartbeat grafts anyone), messages go to at most `D` topic peers
    @Test func testEmptyMeshFallbackIsBounded() throws {
        var router = GossipSubRouter()
        _ = router.join("fruit")
        for peer in try Self.peers(10) { router.handleSubscription(from: peer, topic: "fruit", subscribed: true) }

        let degree = min(GossipSubParameters().meshDegree, 10)
        
        let targets = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)
        #expect(targets.count == degree)
        #expect(router.mesh["fruit"]?.isEmpty == true, "Falling back doesn't graft anyone")
    }

    // MARK: - Gossip

    /// Each heartbeat, recent messages are advertised to `D_lazy` random topic peers outside the mesh
    @Test func testGossipDegree() throws {
        let peers = try Self.peers(20)
        var router = Self.gossipRouter(peers: peers, parameters: .init(gossipDegree: 4))
        _ = router.join("fruit")
        let mesh = try #require(router.mesh["fruit"])
        _ = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)

        let gossiped = Set(router.heartbeat(now: .now).rpcs.filter { !$0.value.control.ihave.isEmpty }.keys)
        #expect(gossiped.count == 4)
        /// ensure we didn't gossip to our mesh peers
        #expect(gossiped.isDisjoint(with: mesh))
    }

    /// Fanout topics are gossiped too, to topic peers outside the fanout
    @Test func testFanoutTopicsAreGossiped() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)
        let fanout = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)

        let gossiped = Set(router.heartbeat(now: .now).rpcs.filter { !$0.value.control.ihave.isEmpty }.keys)
        #expect(gossiped == Set(peers).subtracting(fanout))
    }

    // MARK: - FloodSub peers

    /// FloodSub peers receive every message on their topics, but are never grafted, added to a fanout or gossiped to
    @Test func testFloodSubPeers() throws {
        let floodPeer = try PeerID(.Ed25519)
        let gossipPeers = try Self.peers(8)
        var router = Self.gossipRouter(peers: gossipPeers + [floodPeer])
        router.addPeer(floodPeer, protocolID: FloodSub.multicodec)
        for peer in gossipPeers { router.addPeer(peer, protocolID: GossipSub.multicodec) }

        /// Our fanout excludes the FloodSub peer, but it's still sent the message
        let fanoutTargets = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)
        #expect(router.fanout["fruit"]?.contains(floodPeer) == false)
        #expect(fanoutTargets.contains(floodPeer))

        /// JOIN never grafts the FloodSub peer
        let joined = router.join("fruit")
        #expect(router.mesh["fruit"]?.contains(floodPeer) == false)
        #expect(joined.rpcs[floodPeer] == nil)

        /// Forwarded messages still reach the FloodSub peer
        let forwardTargets = router.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: gossipPeers[0], now: .now)
        #expect(forwardTargets.contains(floodPeer))

        /// The heartbeat never grafts or gossips to the FloodSub peer
        for _ in 0..<3 {
            let outbox = router.heartbeat(now: .now)
            #expect(outbox.rpcs[floodPeer] == nil)
        }

        /// Control messages from a FloodSub peer are ignored
        let reply = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: floodPeer, hasSeen: { _ in false })
        #expect(reply.isEmpty)
        #expect(router.mesh["fruit"]?.contains(floodPeer) == false)
    }

    @Test func testHeartbeatGossipsToPeersOutsideTheMesh() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)
        _ = router.join("fruit")
        let mesh = try #require(router.mesh["fruit"])

        let id = Data("banana".utf8)
        _ = router.route(Self.message("banana"), id: id, topic: "fruit", from: nil, now: .now)
        let outbox = router.heartbeat(now: .now)

        let gossiped = Set(outbox.rpcs.filter { !$0.value.control.ihave.isEmpty }.keys)
        #expect(gossiped == Set(peers).subtracting(mesh))
        #expect(outbox.rpcs.values.flatMap(\.control.ihave).allSatisfy { $0.topicID == "fruit" && $0.messageIds == [id] })

        /// Nothing to gossip means no (empty) IHAVEs
        let quiet = GossipSubRouter(parameters: .init())
        var quietRouter = quiet
        _ = quietRouter.join("fruit")
        #expect(quietRouter.heartbeat(now: .now).isEmpty)
    }

    // MARK: - Outbox

    /// Everything destined for a peer is coalesced into a single RPC (piggybacking control messages on messages)
    @Test func testOutboxCoalescesPerPeer() throws {
        let peer = try PeerID(.Ed25519)
        var outbox = Outbox()
        outbox.graft("fruit", to: peer)
        outbox.send(messages: [Self.message("banana")], to: peer)
        outbox.prune("news", to: peer)

        #expect(outbox.rpcs.count == 1)
        let rpc = try #require(outbox.rpcs[peer])
        #expect(rpc.msgs.count == 1)
        #expect(rpc.control.graft.map(\.topicID) == ["fruit"])
        #expect(rpc.control.prune.map(\.topicID) == ["news"])
    }

    /// Oversized RPCs are split into one RPC per message, and messages that are too large on their own are dropped
    @Test func testFragmentation() {
        let rpc = RPC.with {
            $0.subscriptions = [.with { $0.topicID = "fruit"; $0.subscribe = true }]
            $0.msgs = [
                Self.message(String(repeating: "a", count: 600)),
                Self.message(String(repeating: "b", count: 600)),
                Self.message(String(repeating: "c", count: 5_000)),
            ]
        }
        let (fragments, dropped) = rpc.fragmented(maxSize: 1_000)
        #expect(dropped == 1)
        #expect(fragments.count == 3)
        #expect(fragments[0].subscriptions.count == 1 && fragments[0].msgs.isEmpty)
        #expect(fragments.allSatisfy { ((try? $0.serializedData().count) ?? .max) <= 1_000 })
    }
}
