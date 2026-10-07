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

/// A PubSub routing algorithm (FloodSub, GossipSub, ...).
///
/// Routers are plain state machines, owned and driven by a ``PubSubEngine``. They're told about peers, subscriptions,
/// messages and control traffic, and respond with the RPCs they'd like sent. They never perform I/O themselves, which
/// keeps them deterministic and easy to test.
///
/// The engine takes care of everything routers have in common, framing, signing, signature policies,
/// validation, duplicate suppression, local delivery and subscription announcements.
protocol PubSubRouter: Sendable {
    typealias Instant = ContinuousClock.Instant

    /// The peers we know that are subscribed to `topic`
    func peers(subscribedTo topic: String) -> Set<PeerID>

    /// A peer we're exchanging RPCs with, over the negotiated `protocolID` (ex: `/meshsub/1.2.0` or `/floodsub/1.0.0`)
    ///
    /// - Parameters:
    ///   - outbound: Whether we dialed the connection to the peer (as opposed to the peer dialing us)
    ///   - ip: The IP address the peer connected from (if known)
    mutating func addPeer(_ peer: PeerID, protocolID: String, outbound: Bool, ip: String?)

    /// Forgets everything about a peer that has disconnected
    mutating func removePeer(_ peer: PeerID, now: Instant)

    /// Whether we should process RPCs from this peer at all (ex: GossipSub ignores peers whose score is below its graylist threshold)
    func accepts(rpcFrom peer: PeerID) -> Bool

    /// A peer announced that its subscription to the `topic` has changed.
    mutating func handleSubscription(from peer: PeerID, topic: String, subscribed: Bool)

    /// We subscribed to a topic
    mutating func join(_ topic: String, now: Instant) -> Outbox

    /// We unsubscribed from a topic
    mutating func leave(_ topic: String, now: Instant) -> Outbox

    /// A new (unseen) message, that conforms to its topic's signature policy, has arrived and is about to be validated
    mutating func received(_ message: RPC.Message, id: Data, topic: String, from source: PeerID, now: Instant) -> Outbox

    /// A peer delivered a message we've already seen, or are currently validating
    mutating func duplicate(_ message: RPC.Message, id: Data, topic: String, from peer: PeerID, now: Instant)

    /// A message was rejected (it violated its topic's signature policy, or failed validation)
    mutating func rejected(
        _ message: RPC.Message,
        id: Data,
        topic: String,
        from source: PeerID,
        reason: MessageRejection,
        now: Instant
    )

    /// Returns the peers a (new, valid) message should be sent to.
    ///
    /// - Parameter source: The peer that forwarded the message to us, or `nil` if we're publishing it.
    /// - Note: The engine never sends a message back to its `source` or to its author, so routers don't need to exclude them.
    mutating func route(
        _ message: RPC.Message,
        id: Data,
        topic: String,
        from source: PeerID?,
        now: Instant
    ) -> Set<PeerID>

    /// Handles the control messages (GRAFT, PRUNE, IHAVE, IWANT, ...) in an inbound RPC
    mutating func handleControl(
        _ control: RPC.ControlMessage,
        from peer: PeerID,
        hasSeen: (Data) -> Bool,
        now: Instant
    ) -> Outbox

    /// Periodic maintenance, performed once per heartbeat interval
    mutating func heartbeat(now: Instant) -> Outbox
}

extension PubSubRouter {
    func accepts(rpcFrom peer: PeerID) -> Bool { true }

    mutating func received(_ message: RPC.Message, id: Data, topic: String, from source: PeerID, now: Instant) -> Outbox
    {
        Outbox()
    }

    mutating func duplicate(_ message: RPC.Message, id: Data, topic: String, from peer: PeerID, now: Instant) {}

    mutating func rejected(
        _ message: RPC.Message,
        id: Data,
        topic: String,
        from source: PeerID,
        reason: MessageRejection,
        now: Instant
    ) {}
}

/// Tracks which topics each peer is subscribed to
struct TopicMembership {
    private(set) var peersByTopic: [String: Set<PeerID>] = [:]

    subscript(topic: String) -> Set<PeerID> {
        self.peersByTopic[topic] ?? []
    }

    mutating func update(_ peer: PeerID, topic: String, subscribed: Bool) {
        if subscribed {
            self.peersByTopic[topic, default: []].insert(peer)
        } else {
            self.peersByTopic[topic]?.remove(peer)
            if self.peersByTopic[topic]?.isEmpty == true { self.peersByTopic.removeValue(forKey: topic) }
        }
    }

    mutating func remove(_ peer: PeerID) {
        for topic in self.peersByTopic.keys {
            self.update(peer, topic: topic, subscribed: false)
        }
    }
}
