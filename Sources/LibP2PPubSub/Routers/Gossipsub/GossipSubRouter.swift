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
/// to which we forward full messages.
///
/// For topics we publish to without subscribing, we remember a fanout set of `D` peers (for `fanout_ttl` after our last publish).
/// A random selection of `D_lazy` of the remaining topic peers learn about recent messages via IHAVE gossip each heartbeat,
/// and can request any messages they may have missed with IWANT messages.
///
/// Peers that only speak FloodSub receive every message on the topics they're subscribed to, but are never grafted,
/// added to a fanout, or sent gossip messages.
struct GossipSubRouter: PubSubRouter {

    /// Our GossipSub specific params
    let parameters: GossipSubParameters

    /// Every peer we know to be subscribed to each topic
    private(set) var membership = TopicMembership()

    /// The peers we've grafted into each topic we're subscribed to
    private(set) var mesh: [String: Set<PeerID>] = [:]

    /// The peers we publish to on each topic we're not subscribed to
    private(set) var fanout: [String: Set<PeerID>] = [:]

    /// When we last published to each of our fanout topics
    private(set) var lastPublished: [String: Instant] = [:]

    /// The peers that only speak FloodSub
    private(set) var floodSubPeers: Set<PeerID> = []

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

    /// The peers subscribed to this topic that speak GossipSub (and can therefore be grafted and gossiped to)
    private func gossipPeers(subscribedTo topic: String) -> Set<PeerID> {
        self.membership[topic].subtracting(self.floodSubPeers)
    }

    /// Record whether the peer speaks GossipSub or only FloodSub
    mutating func addPeer(_ peer: PeerID, protocolID: String) {
        guard protocolID == FloodSub.multicodec else {
            self.floodSubPeers.remove(peer)
            return
        }
        self.floodSubPeers.insert(peer)
        /// FloodSub peers can't take part in our meshes or fanouts
        for topic in self.mesh.keys { self.mesh[topic]?.remove(peer) }
        for topic in self.fanout.keys { self.fanout[topic]?.remove(peer) }
    }

    /// Removes the peer from our membership, meshes and fanouts
    mutating func removePeer(_ peer: PeerID) {
        self.membership.remove(peer)
        self.floodSubPeers.remove(peer)
        for topic in self.mesh.keys { self.mesh[topic]?.remove(peer) }
        for topic in self.fanout.keys { self.fanout[topic]?.remove(peer) }
    }

    /// Update the peers subscription status for the specified topic
    mutating func handleSubscription(from peer: PeerID, topic: String, subscribed: Bool) {
        self.membership.update(peer, topic: topic, subscribed: subscribed)
        if !subscribed {
            self.mesh[topic]?.remove(peer)
            self.fanout[topic]?.remove(peer)
        }
    }

    /// We're joining the topic, select up to `D` of the topic's peers and GRAFT them into our new mesh
    mutating func join(_ topic: String) -> Outbox {
        var outbox = Outbox()
        /// ensure we're not already part of this topic
        guard self.mesh[topic] == nil else { return outbox }
        let candidates = self.gossipPeers(subscribedTo: topic)
        /// start with the fanout peers we've been publishing to (if they're still subscribed), we're no longer a fanout publisher
        var selected = (self.fanout.removeValue(forKey: topic) ?? []).intersection(candidates)
        self.lastPublished.removeValue(forKey: topic)
        /// top up with a random selection of the topic's other peers, until we have `D`
        let missing = max(0, self.parameters.meshDegree - selected.count)
        selected.formUnion(candidates.subtracting(selected).shuffled().prefix(missing))
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

    /// Return the set of peers that we should send this message to
    mutating func route(
        _ message: RPC.Message,
        id: Data,
        topic: String,
        from source: PeerID?,
        now: Instant
    ) -> Set<PeerID> {
        /// record the message in our message cache
        self.messageCache.put(id, message: message, topic: topic)

        /// FloodSub peers receive every message on the topics they're subscribed to
        let floodSubTargets = self.membership[topic].intersection(self.floodSubPeers)

        /// Topics we're subscribed to are sent to our mesh
        if let mesh = self.mesh[topic] {
            guard mesh.isEmpty else { return mesh.union(floodSubTargets) }
            /// Our mesh doesn't have any peers yet (ex: we published before our first heartbeat grafted any), so rather than
            /// silently dropping the message, send it to up to `D` random topic peers
            let fallback = self.gossipPeers(subscribedTo: topic).shuffled().prefix(self.parameters.meshDegree)
            return floodSubTargets.union(fallback)
        }

        /// We only ever forward messages for topics we're subscribed to, so this must be one of our own
        guard source == nil else { return floodSubTargets }

        /// Topics we're not subscribed to are sent to the topic's fanout, selecting `D` peers for it if it's empty
        var fanout = (self.fanout[topic] ?? []).intersection(self.gossipPeers(subscribedTo: topic))
        if fanout.isEmpty {
            fanout = Set(self.gossipPeers(subscribedTo: topic).shuffled().prefix(self.parameters.meshDegree))
        }
        self.fanout[topic] = fanout
        self.lastPublished[topic] = now
        return fanout.union(floodSubTargets)
    }

    /// Handle the inbound control message
    mutating func handleControl(_ control: RPC.ControlMessage, from peer: PeerID, hasSeen: (Data) -> Bool) -> Outbox {
        var outbox = Outbox()
        /// FloodSub peers don't speak GossipSub's control protocol
        guard !self.floodSubPeers.contains(peer) else { return outbox }
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

    mutating func heartbeat(now: Instant) -> Outbox {
        var outbox = Outbox()

        /// Perform our mesh maintenance
        for (topic, mesh) in self.mesh {
            if mesh.count < self.parameters.meshDegreeLow {
                /// We need more peers, graft `D - |mesh|` random topic peers
                let candidates = self.gossipPeers(subscribedTo: topic).subtracting(mesh)
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

        /// Perform our fanout maintenance
        for (topic, fanout) in self.fanout {
            /// forget fanout topics we haven't published to within the last `fanout_ttl`
            guard let last = self.lastPublished[topic], last.duration(to: now) < self.parameters.fanoutTTL else {
                self.fanout.removeValue(forKey: topic)
                self.lastPublished.removeValue(forKey: topic)
                continue
            }
            /// drop peers that are no longer subscribed, and top up to `D` with a random selection of the topic's other peers
            let candidates = self.gossipPeers(subscribedTo: topic)
            var kept = fanout.intersection(candidates)
            let missing = max(0, self.parameters.meshDegree - kept.count)
            kept.formUnion(candidates.subtracting(kept).shuffled().prefix(missing))
            self.fanout[topic] = kept
        }

        /// Gossip emission, advertise recent messages (for our mesh and fanout topics) to `D_lazy` random topic peers
        /// outside of the topic's mesh / fanout
        for topic in Set(self.mesh.keys).union(self.fanout.keys) {
            let ids = self.messageCache.gossipIDs(for: topic)
            guard !ids.isEmpty else { continue }
            /// create the ihave with the message IDs
            let iHave = RPC.ControlIHave.with {
                $0.topicID = topic
                $0.messageIds = ids
            }
            /// the peers that already receive full messages for this topic don't need gossip
            let fullPeers = self.mesh[topic] ?? self.fanout[topic] ?? []
            let candidates = self.gossipPeers(subscribedTo: topic).subtracting(fullPeers)
            for peer in candidates.shuffled().prefix(self.parameters.gossipDegree) {
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
