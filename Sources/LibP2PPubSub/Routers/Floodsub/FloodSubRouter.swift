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

/// FloodSub routing, every message for a topic is sent to every peer whos subscribed to that topic.
///
/// FloodSub spec has no control messages and no periodic maintenance, it only needs to know who's subscribed to what.
struct FloodSubRouter: PubSubRouter {
    private(set) var membership = TopicMembership()

    /// Return the members for the topic
    func peers(subscribedTo topic: String) -> Set<PeerID> {
        self.membership[topic]
    }

    /// Remove the peer from the topics membership
    mutating func removePeer(_ peer: PeerID) {
        self.membership.remove(peer)
    }

    /// Update the peers membership within the topic
    mutating func handleSubscription(from peer: PeerID, topic: String, subscribed: Bool) {
        self.membership.update(peer, topic: topic, subscribed: subscribed)
    }

    /// No FloodSub specific join / subscribe message
    mutating func join(_ topic: String) -> Outbox { Outbox() }

    /// No FloodSub specific leave / unsubscribe message
    mutating func leave(_ topic: String) -> Outbox { Outbox() }

    /// Just return the members for the topic
    mutating func route(_ message: RPC.Message, id: Data, topic: String, from source: PeerID?) -> Set<PeerID> {
        self.membership[topic]
    }

    /// No Control messages in FloodSub, return an empty Outbox
    mutating func handleControl(_ control: RPC.ControlMessage, from peer: PeerID, hasSeen: (Data) -> Bool) -> Outbox {
        Outbox()
    }

    /// No FloodSub specific heartbeat logic
    mutating func heartbeat() -> Outbox { Outbox() }
}
