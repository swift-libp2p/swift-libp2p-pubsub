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

/// GossipSub's mesh and gossip parameters.
///
/// The defaults match go-libp2p-pubsub.
/// See the GossipSub Specs...
/// - [v1.0](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md#parameters),
/// - [v1.1](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md)
/// - [v1.2](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md)
public struct GossipSubParameters: Sendable {

    /// `D`, the desired number of peers in each topic mesh.
    public var meshDegree: Int

    /// `D_lo`, below which we graft more peers into a topic mesh.
    public var meshDegreeLow: Int

    /// `D_hi`, above which we prune peers from a topic mesh.
    public var meshDegreeHigh: Int

    /// `mcache_len`, the number of heartbeats a message is kept in the message cache (and can be requested via IWANTs).
    public var historyLength: Int

    /// `mcache_gossip`, the number of heartbeats a message is advertised for via IHAVEs.
    public var historyGossip: Int

    /// `D_lazy`, the number of peers (outside of a topic's mesh and fanout) we gossip IHAVEs to each heartbeat.
    public var gossipDegree: Int

    /// `fanout_ttl`, how long we remember the fanout peers for a topic we publish to without subscribing, after our last publish.
    public var fanoutTTL: Duration

    /// Whether we also speak `/floodsub/1.0.0`, so FloodSub-only peers can join our topics.
    ///
    /// FloodSub peers receive every message on the topics they're subscribed to, but are never grafted into a mesh or sent gossip.
    /// - Note: Don't enable this if the same `Application` also runs a ``FloodSub`` router, both will try to claim the `/floodsub/1.0.0` route.
    public var floodSubCompatible: Bool

    // MARK: GossipSub v1.1

    /// `D_out`, the minimum number of outbound peers (peers we dialed) we try to keep in each topic mesh.
    ///
    /// Outbound connections are harder for an attacker to manufacture, so they help protect our meshes from sybils.
    /// When a mesh is full (`D_hi`), only outbound peers may graft onto it.
    public var outboundDegree: Int

    /// How long a pruned peer must wait before grafting again (sent to the peer in our PRUNE, and honoured for the peers that prune us).
    public var pruneBackoff: Duration

    /// The backoff we request when we prune peers because we're unsubscribing from a topic.
    public var unsubscribeBackoff: Duration

    /// Whether the messages we publish are sent to every peer subscribed to the topic (not just our mesh / fanout).
    ///
    /// Flood publishing gets our own messages out quickly and makes eclipsing us harder, at the cost of some duplicates.
    public var floodPublish: Bool

    /// The fraction of eligible topic peers (outside the mesh / fanout) we gossip to each heartbeat, when it's more than `D_lazy`.
    public var gossipFactor: Double

    /// The most message IDs we advertise in an IHAVE, and the most we'll request from a single peer per heartbeat.
    public var maxIHaveLength: Int

    /// The most IHAVE messages we'll process from a single peer per heartbeat.
    public var maxIHaveMessages: Int

    /// The most times we'll send the same message to a peer in response to its IWANTs.
    public var gossipRetransmission: Int

    /// Whether we take part in peer exchange (PX): we include other topic peers in the PRUNEs we send (when pruning
    /// an oversubscribed mesh, or unsubscribing), and connect to the peers suggested in the PRUNEs we receive.
    ///
    /// Off by default (like go-libp2p-pubsub and rust-libp2p). Without peer scoring, accepting PX lets any peer steer
    /// who we connect to.
    /// - Note: Signed peer records aren't supported yet, so we can only connect to suggested peers whose addresses we already know.
    public var peerExchange: Bool

    /// The most peers we include in a PRUNE's peer exchange.
    public var prunePeers: Int

    /// Peers we always forward messages to (and accept messages from), regardless of our meshes.
    ///
    /// Direct peers are never grafted into a mesh or sent gossip, and are expected to be configured symmetrically.
    /// Each address must include the peer's `/p2p/` component. We reconnect to direct peers every `directConnectTicks` heartbeats.
    public var directPeers: [Multiaddr]

    /// How many heartbeats between attempts to reconnect to our direct peers.
    public var directConnectTicks: Int

    // MARK: GossipSub v1.2

    /// Messages at least this large (in bytes) trigger an IDONTWANT to our mesh peers, so they don't send us a duplicate.
    public var dontWantThreshold: Int

    /// How many heartbeats we honour a peer's IDONTWANT for.
    public var dontWantTTL: Int

    /// The most IDONTWANT messages we'll process from a single peer per heartbeat.
    public var maxDontWantMessages: Int

    /// The most message IDs we'll accept in a single IDONTWANT message.
    public var maxDontWantLength: Int

    public init(
        meshDegree: Int = 6,
        meshDegreeLow: Int = 5,
        meshDegreeHigh: Int = 12,
        historyLength: Int = 5,
        historyGossip: Int = 3,
        gossipDegree: Int = 6,
        fanoutTTL: Duration = .seconds(60),
        floodSubCompatible: Bool = true,
        outboundDegree: Int = 2,
        pruneBackoff: Duration = .seconds(60),
        unsubscribeBackoff: Duration = .seconds(10),
        floodPublish: Bool = true,
        gossipFactor: Double = 0.25,
        maxIHaveLength: Int = 5000,
        maxIHaveMessages: Int = 10,
        gossipRetransmission: Int = 3,
        peerExchange: Bool = false,
        prunePeers: Int = 16,
        directPeers: [Multiaddr] = [],
        directConnectTicks: Int = 300,
        dontWantThreshold: Int = 1024,
        dontWantTTL: Int = 3,
        maxDontWantMessages: Int = 1000,
        maxDontWantLength: Int = 10
    ) {
        precondition(
            0 < meshDegreeLow && meshDegreeLow <= meshDegree && meshDegree <= meshDegreeHigh,
            "GossipSub mesh degrees must satisfy 0 < D_lo <= D <= D_hi"
        )
        precondition(
            0 < historyGossip && historyGossip <= historyLength,
            "GossipSub history parameters must satisfy 0 < mcache_gossip <= mcache_len"
        )
        self.meshDegree = meshDegree
        self.meshDegreeLow = meshDegreeLow
        self.meshDegreeHigh = meshDegreeHigh
        self.historyLength = historyLength
        self.historyGossip = historyGossip
        precondition(gossipDegree >= 0, "GossipSub's gossip degree (D_lazy) can't be negative")
        precondition(fanoutTTL > .zero, "GossipSub's fanout TTL must be greater than zero")
        self.gossipDegree = gossipDegree
        self.fanoutTTL = fanoutTTL
        self.floodSubCompatible = floodSubCompatible

        precondition(
            0 <= outboundDegree && outboundDegree < meshDegreeLow && outboundDegree <= meshDegree / 2,
            "GossipSub's outbound degree must satisfy 0 <= D_out < D_lo and D_out <= D / 2"
        )
        precondition(pruneBackoff >= .zero && unsubscribeBackoff >= .zero, "GossipSub's backoffs can't be negative")
        precondition(0 <= gossipFactor && gossipFactor <= 1, "GossipSub's gossip factor must be between 0 and 1")
        precondition(maxIHaveLength > 0 && maxIHaveMessages > 0, "GossipSub's IHAVE limits must be greater than zero")
        precondition(gossipRetransmission > 0, "GossipSub's gossip retransmission must be greater than zero")
        precondition(prunePeers >= 0, "GossipSub's prune peers can't be negative")
        precondition(directConnectTicks > 0, "GossipSub's direct connect ticks must be greater than zero")
        precondition(dontWantThreshold >= 0 && dontWantTTL > 0, "GossipSub's IDONTWANT threshold and TTL are invalid")
        precondition(maxDontWantMessages > 0 && maxDontWantLength > 0, "GossipSub's IDONTWANT limits must be greater than zero")
        self.outboundDegree = outboundDegree
        self.pruneBackoff = pruneBackoff
        self.unsubscribeBackoff = unsubscribeBackoff
        self.floodPublish = floodPublish
        self.gossipFactor = gossipFactor
        self.maxIHaveLength = maxIHaveLength
        self.maxIHaveMessages = maxIHaveMessages
        self.gossipRetransmission = gossipRetransmission
        self.peerExchange = peerExchange
        self.prunePeers = prunePeers
        self.directPeers = directPeers
        self.directConnectTicks = directConnectTicks
        self.dontWantThreshold = dontWantThreshold
        self.dontWantTTL = dontWantTTL
        self.maxDontWantMessages = maxDontWantMessages
        self.maxDontWantLength = maxDontWantLength
    }
}
