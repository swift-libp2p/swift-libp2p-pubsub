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

/// GossipSub routing ([v1.0 spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md)).
///
/// For each topic we're subscribed to, we try to maintain a mesh of `D` peers (kept between `D_lo` and `D_hi` by our heartbeat)
/// to which we forward full messages. The rest of the topic's peers learn about recent messages via IHAVE gossip, and can
/// request any they missed with IWANT messages.
struct GossipSubRouter: PubSubRouter {

    /// Our GossipSub specific params
    let parameters: GossipSubParameters

    /// Every peer we know to be subscribed to each topic
    private(set) var membership = TopicMembership()

    /// The peers we've grafted into each topic we're subscribed to
    private(set) var mesh: [String: Set<PeerID>] = [:]

    /// Recently seen messages (used for IWANT responses and IHAVE gossip)
    private(set) var messageCache: MessageCache

    init(parameters: GossipSubParameters = .init()) {
        self.parameters = parameters
        self.messageCache = MessageCache(
            historyLength: parameters.historyLength,
            gossipLength: parameters.historyGossip
        )
    }

    /// Every peer subscribed to the this topic
    func peers(subscribedTo topic: String) -> Set<PeerID> {
        self.membership[topic]
    }

    /// Removes the peer from our membership (fanout) and mesh if necessary
    mutating func removePeer(_ peer: PeerID) {
        self.membership.remove(peer)
        for topic in self.mesh.keys { self.mesh[topic]?.remove(peer) }
    }

    /// Update the peers subscription status for the specified topic
    mutating func handleSubscription(from peer: PeerID, topic: String, subscribed: Bool) {
        self.membership.update(peer, topic: topic, subscribed: subscribed)
        if !subscribed { self.mesh[topic]?.remove(peer) }
    }

    /// We're joining the topic, select up to `D` of the topic's peers and GRAFT them into our new mesh
    mutating func join(_ topic: String) -> Outbox {
        var outbox = Outbox()
        /// ensure we're not already part of this topic
        guard self.mesh[topic] == nil else { return outbox }
        /// grab a random meshDegree set of peers from our membership
        let selected = Set(self.membership[topic].shuffled().prefix(self.parameters.meshDegree))
        /// add them to the topics mesh
        self.mesh[topic] = selected
        /// send each of the selected peers a graft message
        /// If they reject the graft by sending a prune, we'll handle that in `handleControl` below
        for peer in selected { outbox.graft(topic, to: peer) }
        /// return the messages
        return outbox
    }

    /// We're leaving / unsubscribing from the topic, send a PRUNE message to every peer in the
    /// topic's mesh and forget it
    mutating func leave(_ topic: String) -> Outbox {
        var outbox = Outbox()
        /// remove the topics mesh
        for peer in self.mesh.removeValue(forKey: topic) ?? [] {
            /// send a prune to each peer in the mesh
            outbox.prune(topic, to: peer)
        }
        return outbox
    }

    /// Return the set of peers that we should forward this message to
    mutating func route(_ message: RPC.Message, id: Data, topic: String, from source: PeerID?) -> Set<PeerID> {
        /// record the message in our message cache
        self.messageCache.put(id, message: message, topic: topic)

        /// Topics we're not subscribed to have no mesh, so (like v1.1's flood publishing) we send to every known topic peer.
        /// We do the same while our mesh is still empty, ex: when publishing before our first heartbeat grafted any peers,
        /// rather than silently dropping the message.
        guard let mesh = self.mesh[topic], !mesh.isEmpty else {
            /// No mesh peers, forward to every peer that we know of subscribed to the topic
            return self.membership[topic]
        }
        /// return just our mesh peers
        return mesh
    }

    /// Handle the inbound control message
    mutating func handleControl(_ control: RPC.ControlMessage, from peer: PeerID, hasSeen: (Data) -> Bool) -> Outbox {
        var outbox = Outbox()
        var reply = RPC.ControlMessage()

        /// GRAFT: add the peer to our mesh if we're subscribed to the topic, no response means acceptance.
        /// Otherwise respond with a PRUNE so the peer removes us from its mesh.
        for graft in control.graft where graft.hasTopicID {
            if self.mesh[graft.topicID] != nil {
                /// update our membership
                self.membership.update(peer, topic: graft.topicID, subscribed: true)
                /// add the peer to our mesh (we'll trim later if necessary)
                self.mesh[graft.topicID]?.insert(peer)
            } else {
                /// we aren't subscribed to this topic, reject the graft by sending a prune
                reply.prune.append(.with { $0.topicID = graft.topicID })
            }
        }

        /// PRUNE: remove the peer from our mesh
        for prune in control.prune where prune.hasTopicID {
            self.mesh[prune.topicID]?.remove(peer)
        }

        /// IHAVE: request the advertised messages we haven't seen, on topics we're subscribed to
        var wanted: [Data] = []
        var alreadyWanted = Set<Data>()
        for iHave in control.ihave where self.mesh[iHave.topicID] != nil {
            for id in iHave.messageIds where !hasSeen(id) && alreadyWanted.insert(id).inserted {
                wanted.append(id)
            }
        }
        /// append the wanted ids to the IWANT field of our ControlMessage
        if !wanted.isEmpty { reply.iwant.append(.with { $0.messageIds = wanted }) }

        /// IWANT: respond with the requested messages we still have cached
        var requested = Set<Data>()
        let messages = control.iwant.flatMap(\.messageIds).compactMap { id -> RPC.Message? in
            guard requested.insert(id).inserted else { return nil }
            return self.messageCache.get(id)
        }

        if !reply.prune.isEmpty || !reply.iwant.isEmpty {
            /// if we have control messages, attach them to our outbox
            outbox.send(control: reply, to: peer)
        }
        /// append our messages (if any) to our outbox
        outbox.send(messages: messages, to: peer)
        return outbox
    }

    mutating func heartbeat() -> Outbox {
        var outbox = Outbox()

        /// Perform our mesh maintenance
        for (topic, mesh) in self.mesh {
            if mesh.count < self.parameters.meshDegreeLow {
                /// We need more peers, graft `D - |mesh|` random topic peers
                let candidates = self.membership[topic].subtracting(mesh)
                for peer in candidates.shuffled().prefix(self.parameters.meshDegree - mesh.count) {
                    /// add the peers to our mesh (rejections will be handled via prunes)
                    self.mesh[topic]?.insert(peer)
                    /// add the graft message to our outbox
                    outbox.graft(topic, to: peer)
                }
            } else if mesh.count > self.parameters.meshDegreeHigh {
                /// We have too many peers, prune `|mesh| - D` random mesh peers
                for peer in mesh.shuffled().prefix(mesh.count - self.parameters.meshDegree) {
                    /// remove the peer from our mesh
                    self.mesh[topic]?.remove(peer)
                    /// add the prune message to our outbox
                    outbox.prune(topic, to: peer)
                }
            }
        }

        /// Gossip emission, advertise recent messages to the topic peers outside of our mesh
        for (topic, mesh) in self.mesh {
            let ids = self.messageCache.gossipIDs(for: topic)
            guard !ids.isEmpty else { continue }
            /// create the ihave with the message IDs
            let iHave = RPC.ControlIHave.with {
                $0.topicID = topic
                $0.messageIds = ids
            }
            /// for each peer subscribed to this topic, not in our mesh
            for peer in self.membership[topic].subtracting(mesh) {
                /// send the iHave control frame
                outbox.send(control: .with { $0.ihave = [iHave] }, to: peer)
            }
        }

        /// Shift our message cache window
        self.messageCache.shift()

        /// Return the outbox
        return outbox
    }
}
