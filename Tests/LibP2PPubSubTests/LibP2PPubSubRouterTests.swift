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
    ///
    /// - Note: Flood publishing is disabled by default here, so our own messages exercise the mesh / fanout routing.
    private static func gossipRouter(
        peers: [PeerID],
        topic: String = "fruit",
        parameters: GossipSubParameters = .init(floodPublish: false)
    ) -> GossipSubRouter {
        var router = GossipSubRouter(parameters: parameters)
        for peer in peers { router.handleSubscription(from: peer, topic: topic, subscribed: true) }
        return router
    }

    // MARK: - JOIN / LEAVE

    @Test func testJoinGraftsUpToDPeers() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)

        let outbox = router.join("fruit", now: .now)
        let mesh = try #require(router.mesh["fruit"])
        #expect(mesh.count == 6)
        #expect(mesh.isSubset(of: Set(peers)))
        /// Every mesh peer is sent a GRAFT, and nobody else is
        #expect(Set(outbox.rpcs.keys) == mesh)
        #expect(outbox.rpcs.values.allSatisfy { $0.control.graft.map(\.topicID) == ["fruit"] })
    }

    @Test func testLeavePrunesTheMesh() throws {
        var router = Self.gossipRouter(peers: try Self.peers(3))
        _ = router.join("fruit", now: .now)
        let mesh = try #require(router.mesh["fruit"])

        let outbox = router.leave("fruit", now: .now)
        #expect(router.mesh["fruit"] == nil)
        #expect(Set(outbox.rpcs.keys) == mesh)
        #expect(outbox.rpcs.values.allSatisfy { $0.control.prune.map(\.topicID) == ["fruit"] })
    }

    // MARK: - Heartbeat mesh maintenance

    @Test func testHeartbeatGraftsWhenBelowDLow() throws {
        var router = GossipSubRouter()
        _ = router.join("fruit", now: .now)
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
        /// Once our mesh is full (D_hi), only outbound peers may graft onto it
        for peer in peers { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true) }
        _ = router.join("fruit", now: .now)

        /// Peers graft themselves onto our mesh until we're over D_hi
        for peer in peers {
            _ = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
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
        _ = router.join("fruit", now: .now)

        /// Grafting onto a topic we're subscribed to adds the peer to our mesh, with no response
        let accepted = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        #expect(router.mesh["fruit"] == [peer])
        #expect(accepted.isEmpty)

        /// Grafting onto a topic we're not subscribed to is ignored (v1.1, a PRUNE could leak our peers via PX)
        let ignored = router.handleControl(.with { $0.graft = [.with { $0.topicID = "news" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        #expect(router.mesh["news"] == nil)
        #expect(ignored.isEmpty)
    }

    @Test func testPrune() throws {
        let peer = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: [peer])
        _ = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"] == [peer])

        let outbox = router.handleControl(.with { $0.prune = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        #expect(router.mesh["fruit"]?.isEmpty == true)
        #expect(outbox.isEmpty)
    }

    @Test func testIHaveRequestsUnseenMessages() throws {
        let peer = try PeerID(.Ed25519)
        var router = GossipSubRouter()
        _ = router.join("fruit", now: .now)
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
            hasSeen: { $0 == seen },
            now: .now
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
            hasSeen: { _ in true },
            now: .now
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
        _ = router.join("fruit", now: .now)
        let mesh = try #require(router.mesh["fruit"])
        #expect(router.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: peers[0], now: .now) == mesh)
    }

    // MARK: - Fanout

    /// Publishing to a topic we're not subscribed to keeps using the same fanout peers, until `fanout_ttl` after our last publish
    @Test func testFanoutLifetime() throws {
        let start = ContinuousClock.now
        var router = Self.gossipRouter(peers: try Self.peers(10), parameters: .init(fanoutTTL: .seconds(60), floodPublish: false))

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

        let outbox = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"] == fanout)
        #expect(Set(outbox.rpcs.keys) == fanout)
        #expect(router.fanout["fruit"] == nil)
        #expect(router.lastPublished["fruit"] == nil)
    }

    /// While our mesh is empty (ex: before our first heartbeat grafts anyone), messages go to at most `D` topic peers
    @Test func testEmptyMeshFallbackIsBounded() throws {
        var router = GossipSubRouter(parameters: .init(floodPublish: false))
        _ = router.join("fruit", now: .now)
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
        _ = router.join("fruit", now: .now)
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
        router.addPeer(floodPeer, protocolID: FloodSub.multicodec, outbound: false)
        for peer in gossipPeers { router.addPeer(peer, protocolID: GossipSub.multicodec, outbound: false) }

        /// Our fanout excludes the FloodSub peer, but it's still sent the message
        let fanoutTargets = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)
        #expect(router.fanout["fruit"]?.contains(floodPeer) == false)
        #expect(fanoutTargets.contains(floodPeer))

        /// JOIN never grafts the FloodSub peer
        let joined = router.join("fruit", now: .now)
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
        let reply = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: floodPeer, hasSeen: { _ in false }, now: .now)
        #expect(reply.isEmpty)
        #expect(router.mesh["fruit"]?.contains(floodPeer) == false)
    }

    @Test func testHeartbeatGossipsToPeersOutsideTheMesh() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers)
        _ = router.join("fruit", now: .now)
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
        _ = quietRouter.join("fruit", now: .now)
        #expect(quietRouter.heartbeat(now: .now).isEmpty)
    }

    // MARK: - GossipSub v1.1

    /// A GRAFT/PRUNE round trip with a v1.1 peer, we honour the backoff it requests, and won't graft it again until it expires
    @Test func testPruneBackoffIsHonoured() throws {
        let start = ContinuousClock.now
        let peer = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: [peer])
        router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true)
        _ = router.join("fruit", now: start)
        #expect(router.mesh["fruit"] == [peer])

        /// The peer prunes us, asking for a 30 second backoff
        _ = router.handleControl(
            .with { $0.prune = [.with { $0.topicID = "fruit"; $0.backoff = 30 }] },
            from: peer,
            hasSeen: { _ in false },
            now: start
        )
        #expect(router.mesh["fruit"]?.isEmpty == true)

        /// Our heartbeat won't graft it while it's backed off...
        _ = router.heartbeat(now: start + .seconds(29))
        #expect(router.mesh["fruit"]?.isEmpty == true)

        /// ... and if it grafts us early, we prune it (extending its backoff)
        let early = router.handleControl(
            .with { $0.graft = [.with { $0.topicID = "fruit" }] },
            from: peer,
            hasSeen: { _ in false },
            now: start + .seconds(29)
        )
        #expect(router.mesh["fruit"]?.isEmpty == true)
        #expect(early.rpcs[peer]?.control.prune.first?.backoff == 60)

        /// Once the (extended) backoff expires, the heartbeat grafts it again
        let outbox = router.heartbeat(now: start + .seconds(90))
        #expect(router.mesh["fruit"] == [peer])
        #expect(outbox.rpcs[peer]?.control.graft.map(\.topicID) == ["fruit"])
    }

    /// Prunes carry our backoff to v1.1 peers (the shorter unsubscribe backoff when we leave), but not to v1.0 peers
    @Test func testPruneBackoffsByVersion() throws {
        let modern = try PeerID(.Ed25519)
        let legacy = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: [modern, legacy])
        router.addPeer(modern, protocolID: GossipSub.v1_1, outbound: false)
        router.addPeer(legacy, protocolID: GossipSub.v1_0, outbound: false)
        _ = router.join("fruit", now: .now)

        let outbox = router.leave("fruit", now: .now)
        let modernPrune = try #require(outbox.rpcs[modern]?.control.prune.first)
        let legacyPrune = try #require(outbox.rpcs[legacy]?.control.prune.first)
        #expect(modernPrune.hasBackoff && modernPrune.backoff == 10)
        #expect(legacyPrune.hasBackoff == false)
        /// We back off both peers locally too
        #expect(router.backoff["fruit"]?.keys.count == 2)
    }

    /// Once our mesh is full (D_hi), only outbound peers may graft onto it
    @Test func testFullMeshOnlyAcceptsOutboundGrafts() throws {
        let meshPeers = try Self.peers(12)
        let inbound = try PeerID(.Ed25519)
        let outbound = try PeerID(.Ed25519)
        /// Only the mesh peers are subscribed when we join, so JOIN can't pick the peers under test
        var router = Self.gossipRouter(peers: meshPeers)
        for peer in meshPeers + [inbound] { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: false) }
        router.addPeer(outbound, protocolID: GossipSub.v1_1, outbound: true)
        _ = router.join("fruit", now: .now)
        for peer in meshPeers {
            _ = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        }
        #expect(router.mesh["fruit"]?.count == 12)

        let rejected = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: inbound, hasSeen: { _ in false }, now: .now)
        #expect(router.mesh["fruit"]?.contains(inbound) == false)
        #expect(rejected.rpcs[inbound]?.control.prune.first?.backoff == 60)

        let accepted = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: outbound, hasSeen: { _ in false }, now: .now)
        #expect(router.mesh["fruit"]?.contains(outbound) == true)
        #expect(accepted.isEmpty)
    }

    /// Pruning an oversubscribed mesh keeps at least D_out outbound peers
    @Test func testOversubscribedPruneKeepsOutboundPeers() throws {
        let inbound = try Self.peers(12)
        let outbound = try Self.peers(3)
        /// A small D_hi, so inbound peers alone can't fill the mesh
        let parameters = GossipSubParameters(meshDegree: 6, meshDegreeLow: 5, meshDegreeHigh: 8, floodPublish: false)
        var router = Self.gossipRouter(peers: inbound, parameters: parameters)
        for peer in inbound { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: false) }
        for peer in outbound { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true) }
        _ = router.join("fruit", now: .now)
        /// Inbound peers fill the mesh up to D_hi, then outbound peers (always accepted) push it over
        for peer in inbound + outbound {
            _ = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        }
        let oversubscribed = try #require(router.mesh["fruit"])
        #expect(oversubscribed.count == 11)
        #expect(oversubscribed.isSuperset(of: outbound))

        /// Run the heartbeat a few times, the kept peers are chosen at random
        for _ in 0..<10 {
            var trial = router
            _ = trial.heartbeat(now: .now)
            let mesh = try #require(trial.mesh["fruit"])
            #expect(mesh.count == 6)
            #expect(mesh.intersection(outbound).count >= 2)
            /// The pruned peers are backed off
            let pruned = oversubscribed.subtracting(mesh)
            #expect(pruned.allSatisfy { trial.backoff["fruit"]?[$0] != nil })
        }
    }

    /// A mesh with enough peers, but too few outbound peers, grafts outbound peers
    @Test func testOutboundQuotaGrafting() throws {
        let inbound = try Self.peers(6)
        let outbound = try Self.peers(2)
        let parameters = GossipSubParameters(meshDegree: 6, meshDegreeLow: 5, meshDegreeHigh: 7, floodPublish: false)
        var router = Self.gossipRouter(peers: inbound, parameters: parameters)
        for peer in inbound { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: false) }
        _ = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"]?.count == 6)

        /// Outbound peers subscribe after we've built our mesh
        for peer in outbound {
            router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true)
            router.handleSubscription(from: peer, topic: "fruit", subscribed: true)
        }
        let outbox = router.heartbeat(now: .now)
        #expect(router.mesh["fruit"]?.isSuperset(of: outbound) == true)
        #expect(router.mesh["fruit"]?.count == 8)
        #expect(Set(outbox.rpcs.filter { !$0.value.control.graft.isEmpty }.keys) == Set(outbound))
        /// another heartbeat and we should prune two inbound peers to settle back into our meshDegree
        let outbox2 = router.heartbeat(now: .now)
        #expect(router.mesh["fruit"]?.count == 6)
        #expect(Set(outbox2.rpcs.filter { !$0.value.control.prune.isEmpty }.keys).intersection(inbound).count == 2)
    }

    /// Our own messages are flood published to every topic peer, while forwarded messages only go to our mesh
    @Test func testFloodPublishing() throws {
        let peers = try Self.peers(10)
        var router = Self.gossipRouter(peers: peers, parameters: .init())
        _ = router.join("fruit", now: .now)
        let mesh = try #require(router.mesh["fruit"])

        #expect(router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now) == Set(peers))
        #expect(router.route(Self.message("b"), id: Data("b".utf8), topic: "fruit", from: peers[0], now: .now) == mesh)
        /// Flood publishing doesn't create a fanout
        _ = router.route(Self.message("c"), id: Data("c".utf8), topic: "news", from: nil, now: .now)
        #expect(router.fanout.isEmpty)
    }

    /// We gossip to `max(D_lazy, gossip_factor * |eligible peers|)` peers
    @Test func testAdaptiveGossip() throws {
        let peers = try Self.peers(50)
        var router = Self.gossipRouter(peers: peers)
        _ = router.join("fruit", now: .now)
        _ = router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: nil, now: .now)

        /// 44 eligible peers outside our mesh, 25% of which is 11 (more than D_lazy's 6)
        let gossiped = router.heartbeat(now: .now).rpcs.filter { !$0.value.control.ihave.isEmpty }
        #expect(gossiped.count == 11)
    }

    /// We process a bounded number of IHAVEs (and request a bounded number of messages) per peer per heartbeat
    @Test func testIHaveLimits() throws {
        let peer = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: [peer], parameters: .init(floodPublish: false, maxIHaveLength: 3, maxIHaveMessages: 2))
        _ = router.join("fruit", now: .now)
        func iHave(_ ids: [String]) -> RPC.ControlMessage {
            .with { $0.ihave = [.with { $0.topicID = "fruit"; $0.messageIds = ids.map { Data($0.utf8) } }] }
        }
        func requested(_ outbox: Outbox) -> Int {
            outbox.rpcs[peer]?.control.iwant.flatMap(\.messageIds).count ?? 0
        }

        /// At most `maxIHaveLength` IDs are requested per heartbeat
        #expect(requested(router.handleControl(iHave(["a", "b", "c", "d", "e"]), from: peer, hasSeen: { _ in false }, now: .now)) == 3)
        #expect(requested(router.handleControl(iHave(["f"]), from: peer, hasSeen: { _ in false }, now: .now)) == 0)

        /// The budget resets every heartbeat, but at most `maxIHaveMessages` IHAVEs are processed per heartbeat
        _ = router.heartbeat(now: .now)
        #expect(requested(router.handleControl(iHave(["g"]), from: peer, hasSeen: { _ in false }, now: .now)) == 1)
        #expect(requested(router.handleControl(iHave(["h"]), from: peer, hasSeen: { _ in false }, now: .now)) == 1)
        #expect(requested(router.handleControl(iHave(["i"]), from: peer, hasSeen: { _ in false }, now: .now)) == 0)
    }

    /// We send a message at most `gossipRetransmission` times to a peer in response to its IWANTs
    @Test func testIWantRetransmissionLimit() throws {
        let peer = try PeerID(.Ed25519)
        var router = GossipSubRouter(parameters: .init(gossipRetransmission: 2))
        let id = Data("banana".utf8)
        _ = router.route(Self.message("banana"), id: id, topic: "fruit", from: nil, now: .now)

        let iWant = RPC.ControlMessage.with { $0.iwant = [.with { $0.messageIds = [id] }] }
        let responses = (0..<3).map { _ in
            router.handleControl(iWant, from: peer, hasSeen: { _ in true }, now: .now).rpcs[peer]?.msgs.count ?? 0
        }
        #expect(responses == [1, 1, 0])
    }

    /// Peer exchange (when enabled), prunes of an oversubscribed mesh suggest other topic peers, and suggested peers are dialed
    @Test func testPeerExchange() throws {
        let peers = try Self.peers(14)
        var router = Self.gossipRouter(peers: peers, parameters: .init(floodPublish: false, peerExchange: true, prunePeers: 4))
        for peer in peers { router.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true) }
        _ = router.join("fruit", now: .now)
        for peer in peers {
            _ = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: peer, hasSeen: { _ in false }, now: .now)
        }

        let prunes = router.heartbeat(now: .now).rpcs.values.compactMap(\.control.prune.first)
        #expect(prunes.count == 8) // 14 -> 6
        #expect(prunes.allSatisfy { $0.peers.count == 4 })

        /// A PRUNE suggesting peers we're not connected to results in dials
        let stranger = try PeerID(.Ed25519)
        let pruner = peers[0]
        let outbox = router.handleControl(
            .with {
                $0.prune = [.with { prune in
                    prune.topicID = "fruit"
                    prune.peers = [.with { $0.peerID = Data(stranger.id) }, .with { $0.peerID = Data(peers[1].id) }]
                }]
            },
            from: pruner,
            hasSeen: { _ in false },
            now: .now
        )
        #expect(outbox.dials == [stranger])

        /// Without peer exchange, we neither send nor accept PX
        var closed = Self.gossipRouter(peers: peers)
        for peer in peers { closed.addPeer(peer, protocolID: GossipSub.v1_1, outbound: true) }
        _ = closed.join("fruit", now: .now)
        let ignored = closed.handleControl(
            .with { $0.prune = [.with { $0.topicID = "fruit"; $0.peers = [.with { $0.peerID = Data(stranger.id) }] }] },
            from: pruner,
            hasSeen: { _ in false },
            now: .now
        )
        #expect(ignored.dials.isEmpty)
    }

    /// Direct peers receive every message on their topics, are never meshed, have their GRAFTs pruned, and are reconnected
    @Test func testDirectPeers() throws {
        let direct = try PeerID(.Ed25519)
        let others = try Self.peers(8)
        let address = try Multiaddr("/ip4/127.0.0.1/tcp/4001/p2p/\(direct.b58String)")
        var router = Self.gossipRouter(peers: others + [direct], parameters: .init(floodPublish: false, directPeers: [address]))
        #expect(router.directPeers == [direct])

        /// We dial our (disconnected) direct peers on our first heartbeat
        #expect(router.heartbeat(now: .now).dials == [direct])
        router.addPeer(direct, protocolID: GossipSub.v1_1, outbound: true)

        _ = router.join("fruit", now: .now)
        #expect(router.mesh["fruit"]?.contains(direct) == false)
        #expect(router.route(Self.message("a"), id: Data("a".utf8), topic: "fruit", from: others[0], now: .now).contains(direct))

        let graft = router.handleControl(.with { $0.graft = [.with { $0.topicID = "fruit" }] }, from: direct, hasSeen: { _ in false }, now: .now)
        #expect(graft.rpcs[direct]?.control.prune.first?.peers.isEmpty == true)
        #expect(router.mesh["fruit"]?.contains(direct) == false)

        /// Direct peers are never gossiped to
        for _ in 0..<3 { #expect(router.heartbeat(now: .now).rpcs[direct]?.control.ihave.isEmpty ?? true) }
    }

    // MARK: - GossipSub v1.2

    /// We send IDONTWANTs for large messages to our v1.2 mesh peers (except the message's source and author)
    @Test func testSendingIDontWant() throws {
        let v12 = try Self.peers(3)
        let v11 = try PeerID(.Ed25519)
        var router = Self.gossipRouter(peers: v12 + [v11])
        for peer in v12 { router.addPeer(peer, protocolID: GossipSub.v1_2, outbound: false) }
        router.addPeer(v11, protocolID: GossipSub.v1_1, outbound: false)
        _ = router.join("fruit", now: .now)

        let large = Self.message(String(repeating: "a", count: 1024))
        let id = Data("large".utf8)
        let outbox = router.received(large, id: id, topic: "fruit", from: v12[0])
        #expect(Set(outbox.rpcs.keys) == Set(v12[1...]))
        #expect(outbox.rpcs.values.allSatisfy { $0.control.idontwant.flatMap(\.messageIds) == [id] })

        #expect(router.received(Self.message("small"), id: Data("small".utf8), topic: "fruit", from: v12[0]).isEmpty)
    }

    /// Peers that send us IDONTWANT don't receive the message (forwarded or via IWANT) for `dontWantTTL` heartbeats
    @Test func testReceivingIDontWant() throws {
        let peers = try Self.peers(3)
        var router = Self.gossipRouter(peers: peers, parameters: .init(floodPublish: false, dontWantTTL: 2))
        for peer in peers { router.addPeer(peer, protocolID: GossipSub.v1_2, outbound: false) }
        _ = router.join("fruit", now: .now)
        let picky = peers[0]
        let id = Data("banana".utf8)

        _ = router.handleControl(.with { $0.idontwant = [.with { $0.messageIds = [id] }] }, from: picky, hasSeen: { _ in true }, now: .now)
        #expect(router.route(Self.message("banana"), id: id, topic: "fruit", from: peers[1], now: .now).contains(picky) == false)
        let iWant = router.handleControl(.with { $0.iwant = [.with { $0.messageIds = [id] }] }, from: picky, hasSeen: { _ in true }, now: .now)
        #expect(iWant.rpcs[picky]?.msgs.isEmpty ?? true)

        /// After `dontWantTTL` heartbeats the IDONTWANT expires
        _ = router.heartbeat(now: .now)
        #expect(router.unwanted[picky]?[id] != nil)
        _ = router.heartbeat(now: .now)
        #expect(router.unwanted[picky]?[id] == nil)
    }

    /// IDONTWANT survives a serialization round trip
    @Test func testIDontWantWireFormat() throws {
        let rpc = RPC.with { $0.control = .with { $0.idontwant = [.with { $0.messageIds = [Data("a".utf8), Data("b".utf8)] }] } }
        let decoded = try RPC(serializedBytes: try rpc.serializedData())
        #expect(decoded == rpc)
        #expect(decoded.control.idontwant.first?.messageIds == [Data("a".utf8), Data("b".utf8)])
        /// ControlMessage field 5, a length delimited `ControlIDontWant`, containing `messageIDs` (field 1)
        let control = try RPC.ControlMessage.with { $0.idontwant = [.with { $0.messageIds = [Data([0xAB])] }] }.serializedData()
        #expect(Array(control) == [0x2A, 0x03, 0x0A, 0x01, 0xAB])
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
