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

/// GossipSub's mesh and gossip parameters.
///
/// The defaults match go-libp2p-pubsub and rust-libp2p.
/// See the [GossipSub v1.0 spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md#parameters).
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

    public init(
        meshDegree: Int = 6,
        meshDegreeLow: Int = 5,
        meshDegreeHigh: Int = 12,
        historyLength: Int = 5,
        historyGossip: Int = 3
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
    }
}
