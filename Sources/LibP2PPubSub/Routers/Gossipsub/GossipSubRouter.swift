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

/// GossipSub routing covering the following 3 versions of the spec...
/// - [v1.0](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md)
/// - [v1.1](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md)
///   (optional peer ``GossipSubParameters/scoring``)
/// - [v1.2](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md)
///
/// For each topic we're subscribed to, we try to maintain a mesh of `D` peers (kept between `D_lo` and `D_hi`, with at least
/// `D_out` outbound peers, by our heartbeat) to which we forward full messages. For topics we publish to without subscribing,
/// we remember a fanout set of `D` peers (for `fanout_ttl` after our last publish). Every heartbeat, a random selection of the
/// remaining topic peers learn about recent messages via IHAVE gossip, and can request any they missed with IWANT messages.
///
/// - Pruned peers are backed off, and aren't grafted again (in either direction) until their backoff expires.
/// - Our own messages are flood published to every topic peer (unless disabled).
/// - Peers that speak GossipSub v1.2 are told (via IDONTWANT) not to send us large messages we've already received.
/// - Peers that only speak FloodSub, and direct peers, receive every message on the topics they're subscribed to, but are never
///   grafted, added to a fanout, or sent gossip.
/// - With peer scoring enabled, negative scoring peers are pruned and never grafted, and peers below the
///   ``PeerScoreThresholds`` are progressively excluded from gossip, publishing, peer exchange and finally everything.
struct GossipSubRouter: PubSubRouter {

    /// The protocol a peer speaks with us, which determines the features we can use with it
    enum PeerProtocol: Int, Comparable {
        case floodSub
        case gossipSubV1_0
        case gossipSubV1_1
        case gossipSubV1_2

        init(_ protocolID: String) {
            switch protocolID {
            case FloodSub.multicodec: self = .floodSub
            case GossipSub.v1_2: self = .gossipSubV1_2
            case GossipSub.v1_1: self = .gossipSubV1_1
            default: self = .gossipSubV1_0
            }
        }

        /// GossipSub v1.1 introduced prune backoffs and peer exchange
        var supportsBackoff: Bool { self >= .gossipSubV1_1 }

        /// GossipSub v1.2 introduced IDONTWANT
        var supportsDontWant: Bool { self >= .gossipSubV1_2 }

        static func < (lhs: PeerProtocol, rhs: PeerProtocol) -> Bool { lhs.rawValue < rhs.rawValue }
    }

    struct PeerDetails {
        let protocolKind: PeerProtocol
        /// Whether we dialed the connection to this peer
        let outbound: Bool
    }

    /// Our GossipSub specific params
    let parameters: GossipSubParameters

    /// Peers we always forward to, and never mesh with
    let directPeers: Set<PeerID>

    /// Every peer we know to be subscribed to each topic
    private(set) var membership = TopicMembership()

    /// The peers we've grafted into each topic we're subscribed to
    private(set) var mesh: [String: Set<PeerID>] = [:]

    /// The peers we publish to on each topic we're not subscribed to
    private(set) var fanout: [String: Set<PeerID>] = [:]

    /// When we last published to each of our fanout topics
    private(set) var lastPublished: [String: Instant] = [:]

    /// The peers we're exchanging RPCs with, and what they support
    private(set) var peers: [PeerID: PeerDetails] = [:]

    /// When each peer's backoff (per topic) expires. Until then we won't graft the peer, or accept its grafts.
    private(set) var backoff: [String: [PeerID: Instant]] = [:]

    /// The message IDs each peer has asked us not to send it (IDONTWANT), and the heartbeat they were received on
    private(set) var unwanted: [PeerID: [Data: Int]] = [:]

    /// Recently seen messages (used for IWANT responses and IHAVE gossip)
    private(set) var messageCache: MessageCache

    /// Our peers' scores, when peer scoring is enabled
    private(set) var scores: PeerScore?

    /// The messages peers have promised us (by advertising them in an IHAVE we answered with an IWANT), and when each
    /// peer's promise expires. Broken promises count towards the peer's behavioural penalty.
    private(set) var promises: [Data: [PeerID: Instant]] = [:]

    /// The number of heartbeats we've performed
    private(set) var ticks: Int = 0

    /// Per heartbeat counters, used to bound the gossip work a single peer can cause us
    private var iHavesReceived: [PeerID: Int] = [:]
    private var iWantsRequested: [PeerID: Int] = [:]
    private var dontWantsReceived: [PeerID: Int] = [:]

    init(parameters: GossipSubParameters = .init()) {
        self.parameters = parameters
        self.directPeers = Set(
            parameters.directPeers.compactMap { address in
                address.getPeerIDString().flatMap { try? PeerID(cid: $0) }
            }
        )
        self.messageCache = MessageCache(
            historyLength: parameters.historyLength,
            gossipLength: parameters.historyGossip
        )
        self.scores = parameters.scoring.map { PeerScore(parameters: $0.parameters) }
    }

    // MARK: - Peers

    /// The peers that only speak FloodSub
    var floodSubPeers: Set<PeerID> {
        Set(self.peers.filter { $0.value.protocolKind == .floodSub }.keys)
    }

    /// Every peer subscribed to the this topic
    func peers(subscribedTo topic: String) -> Set<PeerID> {
        self.membership[topic]
    }

    private func isFloodSub(_ peer: PeerID) -> Bool {
        self.peers[peer]?.protocolKind == .floodSub
    }

    private func isOutbound(_ peer: PeerID) -> Bool {
        self.peers[peer]?.outbound ?? false
    }

    private func isBackedOff(_ peer: PeerID, from topic: String, now: Instant) -> Bool {
        guard let expiry = self.backoff[topic]?[peer] else { return false }
        return now < expiry
    }

    /// The peers subscribed to this topic that can be grafted or gossiped to (they speak GossipSub, and aren't direct peers)
    private func gossipPeers(subscribedTo topic: String) -> Set<PeerID> {
        self.membership[topic].filter { !self.isFloodSub($0) && !self.directPeers.contains($0) }
    }

    /// The topic peers we could graft into its mesh right now (they aren't backed off, and don't have a negative score)
    private func graftCandidates(for topic: String, now: Instant) -> Set<PeerID> {
        self.gossipPeers(subscribedTo: topic)
            .subtracting(self.mesh[topic] ?? [])
            .filter { !self.isBackedOff($0, from: topic, now: now) && self.score(of: $0) >= 0 }
    }

    /// Record the protocol the peer speaks, and whether the connection is outbound
    mutating func addPeer(_ peer: PeerID, protocolID: String, outbound: Bool, ip: String?) {
        let details = PeerDetails(protocolKind: PeerProtocol(protocolID), outbound: outbound)
        self.peers[peer] = details
        self.scores?.addPeer(peer, ip: ip)
        guard details.protocolKind == .floodSub else { return }
        /// FloodSub peers can't take part in our meshes or fanouts
        for topic in self.mesh.keys { self.removeFromMesh(peer, topic: topic) }
        for topic in self.fanout.keys { self.fanout[topic]?.remove(peer) }
    }

    /// Removes the peer from our membership, meshes and fanouts
    /// - Note: Like go-libp2p-pubsub, we remember the peer's backoffs (and, when scoring, possibly its score) in case it reconnects
    mutating func removePeer(_ peer: PeerID, now: Instant) {
        /// Our score handles the peer leaving our meshes itself
        self.scores?.removePeer(peer, now: now)
        self.membership.remove(peer)
        self.peers.removeValue(forKey: peer)
        self.unwanted.removeValue(forKey: peer)
        for topic in self.mesh.keys { self.mesh[topic]?.remove(peer) }
        for topic in self.fanout.keys { self.fanout[topic]?.remove(peer) }
    }

    /// Update the peers subscription status for the specified topic
    mutating func handleSubscription(from peer: PeerID, topic: String, subscribed: Bool) {
        self.membership.update(peer, topic: topic, subscribed: subscribed)
        if !subscribed {
            self.removeFromMesh(peer, topic: topic)
            self.fanout[topic]?.remove(peer)
        }
    }

    // MARK: - Scoring

    /// The peer's score (always 0 when scoring is disabled)
    func score(of peer: PeerID) -> Double {
        self.scores?.score(of: peer) ?? 0
    }

    /// Whether the peer's score is at least the specified threshold (always true when scoring is disabled)
    private func scoreMeets(_ peer: PeerID, _ threshold: KeyPath<PeerScoreThresholds, Double>) -> Bool {
        guard let thresholds = self.parameters.scoring?.thresholds else { return true }
        return self.score(of: peer) >= thresholds[keyPath: threshold]
    }

    /// Peers whose score falls below our graylist threshold are ignored entirely
    func accepts(rpcFrom peer: PeerID) -> Bool {
        self.scoreMeets(peer, \.graylistThreshold)
    }

    // MARK: - Grafting & Pruning

    private mutating func addToMesh(_ peer: PeerID, topic: String, now: Instant) {
        guard self.mesh[topic]?.insert(peer).inserted == true else { return }
        self.scores?.graft(peer, topic: topic, now: now)
    }

    private mutating func removeFromMesh(_ peer: PeerID, topic: String) {
        guard self.mesh[topic]?.remove(peer) != nil else { return }
        self.scores?.prune(peer, topic: topic)
    }

    /// Backs off `peer` for `duration`, keeping any longer backoff already in place
    private mutating func addBackoff(_ peer: PeerID, topic: String, duration: Duration, now: Instant) {
        let expiry = now + duration
        if let existing = self.backoff[topic]?[peer], existing >= expiry { return }
        self.backoff[topic, default: [:]][peer] = expiry
    }

    /// Prunes `peer` from the topic's mesh, backs it off, and sends it a PRUNE with our backoff (and PX peers, if enabled)
    ///
    /// - Parameters:
    ///   - peerExchange: Whether this prune may carry PX (never when rejecting a misbehaving or direct peer)
    ///   - unsubscribing: Whether we're pruning because we're leaving the topic (which uses the shorter unsubscribe backoff)
    private mutating func prune(
        _ peer: PeerID,
        from topic: String,
        peerExchange: Bool,
        unsubscribing: Bool = false,
        now: Instant,
        into outbox: inout Outbox
    ) {
        self.removeFromMesh(peer, topic: topic)
        let duration = unsubscribing ? self.parameters.unsubscribeBackoff : self.parameters.pruneBackoff
        self.addBackoff(peer, topic: topic, duration: duration, now: now)

        /// GossipSub v1.0 peers don't understand backoffs or PX
        guard self.peers[peer]?.protocolKind.supportsBackoff ?? false else {
            outbox.prune(topic, to: peer)
            return
        }
        var exchanged: [PeerID] = []
        if peerExchange && self.parameters.peerExchange {
            /// we never suggest peers with a negative score
            let candidates = self.gossipPeers(subscribedTo: topic).subtracting([peer]).filter {
                self.score(of: $0) >= 0
            }
            exchanged = Array(candidates.shuffled().prefix(self.parameters.prunePeers))
        }
        /// The engine attaches the suggested peers' signed records (from our peer store), so the peer can connect to them
        outbox.prune(topic, to: peer, backoff: duration, peers: exchanged)
    }

    /// We're joining the topic, select up to `D` of the topic's peers and GRAFT them into our new mesh
    mutating func join(_ topic: String, now: Instant) -> Outbox {
        var outbox = Outbox()
        /// ensure we're not already part of this topic
        guard self.mesh[topic] == nil else { return outbox }
        let candidates = self.graftCandidates(for: topic, now: now)
        /// start with the fanout peers we've been publishing to (if they're still eligible), we're no longer a fanout publisher
        var selected = (self.fanout.removeValue(forKey: topic) ?? []).intersection(candidates)
        self.lastPublished.removeValue(forKey: topic)
        /// top up with a random selection of the topic's other peers, until we have `D`
        let missing = max(0, self.parameters.meshDegree - selected.count)
        selected.formUnion(candidates.subtracting(selected).shuffled().prefix(missing))
        /// add them to the topics mesh
        self.mesh[topic] = []
        for peer in selected { self.addToMesh(peer, topic: topic, now: now) }
        /// send each of the selected peers a graft message
        /// If they reject the graft by sending a prune, we'll handle that in `handleControl` below
        for peer in selected { outbox.graft(topic, to: peer) }
        /// return the messages
        return outbox
    }

    /// We're leaving / unsubscribing from the topic, PRUNE every peer in the topic's mesh and forget it
    mutating func leave(_ topic: String, now: Instant) -> Outbox {
        var outbox = Outbox()
        for peer in self.mesh[topic] ?? [] {
            self.prune(peer, from: topic, peerExchange: true, unsubscribing: true, now: now, into: &outbox)
        }
        self.mesh.removeValue(forKey: topic)
        return outbox
    }

    // MARK: - Messages

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

        /// a message from a peer that passed validation, credit its (and near-first) deliveries, and fulfil any promises
        if let source {
            self.scores?.delivered(id, topic: topic, from: source, now: now)
            self.promises.removeValue(forKey: id)
        }

        /// FloodSub and direct peers receive every message on the topics they're subscribed to
        var targets = self.membership[topic].filter {
            self.directPeers.contains($0) || (self.isFloodSub($0) && self.scoreMeets($0, \.publishThreshold))
        }

        if source == nil && self.parameters.floodPublish {
            /// We flood publish our own messages to every peer subscribed to the topic (above our publish threshold)
            targets.formUnion(self.membership[topic].filter { self.scoreMeets($0, \.publishThreshold) })
        } else if let mesh = self.mesh[topic] {
            /// Topics we're subscribed to are sent to our mesh
            if mesh.isEmpty {
                /// Our mesh doesn't have any peers yet (ex: before our first heartbeat grafted any), so rather than
                /// silently dropping the message, send it to up to `D` random topic peers
                let fallback = self.gossipPeers(subscribedTo: topic).filter { self.score(of: $0) >= 0 }
                targets.formUnion(fallback.shuffled().prefix(self.parameters.meshDegree))
            } else {
                targets.formUnion(mesh)
            }
        } else if source == nil {
            /// Topics we're not subscribed to are sent to the topic's fanout, selecting `D` peers for it if it's empty
            let candidates = self.gossipPeers(subscribedTo: topic).filter { self.scoreMeets($0, \.publishThreshold) }
            var fanout = (self.fanout[topic] ?? []).intersection(candidates)
            if fanout.isEmpty {
                fanout = Set(candidates.shuffled().prefix(self.parameters.meshDegree))
            }
            self.fanout[topic] = fanout
            self.lastPublished[topic] = now
            targets.formUnion(fanout)
        }

        /// Don't send the message to peers that told us they don't want it
        return targets.filter { self.unwanted[$0]?[id] == nil }
    }

    /// A new message is about to be validated, start tracking its deliveries, fulfill any promises of it, and tell our
    /// GossipSub v1.2 mesh peers not to send us this (large) message, since we already have it
    mutating func received(_ message: RPC.Message, id: Data, topic: String, from source: PeerID, now: Instant) -> Outbox
    {
        self.scores?.validationStarted(id, topic: topic, now: now)
        self.promises.removeValue(forKey: id)

        var outbox = Outbox()
        guard message.data.count >= self.parameters.dontWantThreshold, let mesh = self.mesh[topic] else {
            return outbox
        }
        for peer in mesh where peer != source && !(message.from == peer) {
            guard self.peers[peer]?.protocolKind.supportsDontWant ?? false else { continue }
            outbox.dontWant([id], to: peer)
        }
        return outbox
    }

    mutating func duplicate(_ message: RPC.Message, id: Data, topic: String, from peer: PeerID, now: Instant) {
        self.scores?.duplicate(id, topic: topic, from: peer, now: now)
    }

    mutating func rejected(
        _ message: RPC.Message,
        id: Data,
        topic: String,
        from source: PeerID,
        reason: MessageRejection,
        now: Instant
    ) {
        self.scores?.rejected(id, topic: topic, from: source, reason: reason, now: now)
        /// Like go-libp2p-pubsub, a message with a bad signature doesn't fulfill a promise (the promiser could have forged it)
        if reason != .invalidSignature { self.promises.removeValue(forKey: id) }
    }

    // MARK: - Control messages

    /// Handle the inbound control message
    mutating func handleControl(
        _ control: RPC.ControlMessage,
        from peer: PeerID,
        hasSeen: (Data) -> Bool,
        now: Instant
    ) -> Outbox {
        var outbox = Outbox()
        /// FloodSub peers don't speak GossipSub's control protocol
        guard !self.isFloodSub(peer) else { return outbox }

        self.handleGrafts(control.graft, from: peer, now: now, into: &outbox)
        self.handlePrunes(control.prune, from: peer, now: now, into: &outbox)
        self.handleDontWants(control.idontwant, from: peer)

        /// We neither answer, nor act on, the gossip of peers below our gossip threshold
        guard self.scoreMeets(peer, \.gossipThreshold) else { return outbox }
        var reply = RPC.ControlMessage()
        if let iWant = self.handleIHaves(control.ihave, from: peer, hasSeen: hasSeen, now: now) {
            reply.iwant = [iWant]
        }
        if !reply.iwant.isEmpty { outbox.send(control: reply, to: peer) }
        outbox.send(messages: self.handleIWants(control.iwant, from: peer), to: peer)
        return outbox
    }

    /// GRAFT: add the peer to our mesh (no response means acceptance), unless...
    /// - we're not subscribed to the topic: the GRAFT is ignored (v1.1, so we don't leak our peers via PX)
    /// - it's a direct peer: we PRUNE it, without PX
    /// - it's backed off: we penalize it (twice if it's flooding us with GRAFTs) and PRUNE it (extending its backoff), without PX
    /// - it has a negative score: we PRUNE it, without PX
    /// - our mesh is full and it's an inbound peer: we PRUNE it, with PX
    private mutating func handleGrafts(
        _ grafts: [RPC.ControlGraft],
        from peer: PeerID,
        now: Instant,
        into outbox: inout Outbox
    ) {
        for graft in grafts where graft.hasTopicID {
            let topic = graft.topicID
            guard let mesh = self.mesh[topic], !mesh.contains(peer) else { continue }
            if self.directPeers.contains(peer) {
                self.prune(peer, from: topic, peerExchange: false, now: now, into: &outbox)
                continue
            }
            if let expiry = self.backoff[topic]?[peer], now < expiry {
                self.scores?.addPenalty(peer)
                /// grafting within `graftFloodThreshold` of being pruned is penalized again
                let prunedAt = expiry - self.parameters.pruneBackoff
                if now < prunedAt + self.parameters.graftFloodThreshold { self.scores?.addPenalty(peer) }
                self.prune(peer, from: topic, peerExchange: false, now: now, into: &outbox)
                continue
            }
            if self.score(of: peer) < 0 {
                self.prune(peer, from: topic, peerExchange: false, now: now, into: &outbox)
                continue
            }
            if mesh.count >= self.parameters.meshDegreeHigh && !self.isOutbound(peer) {
                self.prune(peer, from: topic, peerExchange: true, now: now, into: &outbox)
                continue
            }
            /// update our membership, and add the peer to our mesh
            self.membership.update(peer, topic: topic, subscribed: true)
            self.addToMesh(peer, topic: topic, now: now)
        }
    }

    /// PRUNE: remove the peer from our mesh, honour its backoff, and (if enabled, and the peer's score is at least our
    /// accept PX threshold) connect to the peers it suggested.
    /// Like go-libp2p-pubsub, a suggested peer with an invalid signed record (or one belonging to another peer) is ignored,
    /// and a suggested peer without a record is dialed using the addresses in our peer store.
    private mutating func handlePrunes(
        _ prunes: [RPC.ControlPrune],
        from peer: PeerID,
        now: Instant,
        into outbox: inout Outbox
    ) {
        for prune in prunes where prune.hasTopicID {
            self.removeFromMesh(peer, topic: prune.topicID)
            let requested =
                prune.hasBackoff && prune.backoff > 0 ? Duration.seconds(Int64(clamping: prune.backoff)) : nil
            self.addBackoff(peer, topic: prune.topicID, duration: requested ?? self.parameters.pruneBackoff, now: now)

            guard self.parameters.peerExchange, self.scoreMeets(peer, \.acceptPXThreshold) else { continue }
            for info in prune.peers.prefix(self.parameters.prunePeers) {
                guard let suggested = try? PeerID(fromBytesID: info.peerID.byteArray), suggested != peer else {
                    continue
                }
                guard self.peers[suggested] == nil else { continue }
                guard info.hasSignedPeerRecord else {
                    outbox.dial(suggested)
                    continue
                }
                guard let signed = try? SignedPeerRecord(envelope: info.signedPeerRecord), signed.peer == suggested
                else {
                    continue
                }
                outbox.dial(suggested, record: signed.record)
            }
        }
    }

    /// IHAVE: request the advertised messages we haven't seen, on topics we're subscribed to.
    /// We process at most `maxIHaveMessages` IHAVEs, and request at most `maxIHaveLength` messages, per peer per heartbeat.
    /// When scoring, the peer promises (one random message of) each IWANT we send it, breaking the promise is penalized.
    private mutating func handleIHaves(
        _ iHaves: [RPC.ControlIHave],
        from peer: PeerID,
        hasSeen: (Data) -> Bool,
        now: Instant
    ) -> RPC.ControlIWant? {
        guard !iHaves.isEmpty else { return nil }
        self.iHavesReceived[peer, default: 0] += 1
        let requested = self.iWantsRequested[peer, default: 0]
        /// ensure we haven't exceeded our limits for this peer
        guard self.iHavesReceived[peer, default: 0] <= self.parameters.maxIHaveMessages,
            requested < self.parameters.maxIHaveLength
        else { return nil }

        var wanted: [Data] = []
        var alreadyWanted = Set<Data>()
        for iHave in iHaves where self.mesh[iHave.topicID] != nil {
            for id in iHave.messageIds where !hasSeen(id) && alreadyWanted.insert(id).inserted {
                wanted.append(id)
            }
        }
        /// stay within our per heartbeat budget (requesting a random selection if we'd exceed it)
        let budget = self.parameters.maxIHaveLength - requested
        if wanted.count > budget { wanted = Array(wanted.shuffled().prefix(budget)) }
        guard !wanted.isEmpty else { return nil }
        self.iWantsRequested[peer, default: 0] += wanted.count

        if self.scores != nil, let promised = wanted.randomElement(), self.promises[promised]?[peer] == nil {
            self.promises[promised, default: [:]][peer] = now + self.parameters.iWantFollowupTime
        }
        return .with { $0.messageIds = wanted }
    }

    /// IWANT: respond with the requested messages we still have cached, unless the peer told us it doesn't want them,
    /// or we've already sent it the message `gossipRetransmission` times
    private mutating func handleIWants(_ iWants: [RPC.ControlIWant], from peer: PeerID) -> [RPC.Message] {
        var requested = Set<Data>()
        var messages: [RPC.Message] = []
        for id in iWants.flatMap(\.messageIds) where requested.insert(id).inserted {
            guard self.unwanted[peer]?[id] == nil else { continue }
            guard let (message, transmissions) = self.messageCache.get(id, for: peer),
                transmissions <= self.parameters.gossipRetransmission
            else { continue }
            messages.append(message)
        }
        return messages
    }

    /// IDONTWANT: remember the messages the peer doesn't want (for `dontWantTTL` heartbeats).
    /// We process at most `maxDontWantMessages` IDONTWANTs per peer per heartbeat, of up to `maxDontWantLength` IDs each.
    private mutating func handleDontWants(_ dontWants: [RPC.ControlIDontWant], from peer: PeerID) {
        for dontWant in dontWants {
            self.dontWantsReceived[peer, default: 0] += 1
            guard self.dontWantsReceived[peer, default: 0] <= self.parameters.maxDontWantMessages else { return }
            for id in dontWant.messageIds.prefix(self.parameters.maxDontWantLength) {
                self.unwanted[peer, default: [:]][id] = self.ticks
            }
        }
    }

    // MARK: - Heartbeat

    mutating func heartbeat(now: Instant) -> Outbox {
        var outbox = Outbox()
        self.ticks += 1

        /// Decay our scores, and penalize the peers that broke their promises
        self.scores?.refresh(now: now)
        self.applyBrokenPromisePenalties(now: now)

        /// Reset our per heartbeat counters, and expire old IDONTWANTs and backoffs
        self.iHavesReceived.removeAll()
        self.iWantsRequested.removeAll()
        self.dontWantsReceived.removeAll()
        for peer in self.unwanted.keys {
            self.unwanted[peer] = self.unwanted[peer]?.filter { self.ticks - $0.value < self.parameters.dontWantTTL }
        }
        for topic in self.backoff.keys {
            self.backoff[topic] = self.backoff[topic]?.filter { $0.value > now }
            if self.backoff[topic]?.isEmpty == true { self.backoff.removeValue(forKey: topic) }
        }

        /// Reconnect to any direct peers we've lost (on our first heartbeat, and every `directConnectTicks` thereafter)
        if (self.ticks - 1) % self.parameters.directConnectTicks == 0 {
            for peer in self.directPeers where self.peers[peer] == nil { outbox.dial(peer) }
        }

        /// Perform our mesh maintenance
        for topic in self.mesh.keys {
            self.maintainMesh(for: topic, now: now, into: &outbox)
        }

        /// Perform our fanout maintenance
        for (topic, fanout) in self.fanout {
            /// forget fanout topics we haven't published to within the last `fanout_ttl`
            guard let last = self.lastPublished[topic], last.duration(to: now) < self.parameters.fanoutTTL else {
                self.fanout.removeValue(forKey: topic)
                self.lastPublished.removeValue(forKey: topic)
                continue
            }
            /// drop peers that are no longer subscribed (or have fallen below our publish threshold), and top up to `D`
            /// with a random selection of the topic's other peers
            let candidates = self.gossipPeers(subscribedTo: topic).filter { self.scoreMeets($0, \.publishThreshold) }
            var kept = fanout.intersection(candidates)
            let missing = max(0, self.parameters.meshDegree - kept.count)
            kept.formUnion(candidates.subtracting(kept).shuffled().prefix(missing))
            self.fanout[topic] = kept
        }

        /// Gossip emission, advertise recent messages (for our mesh and fanout topics)
        for topic in Set(self.mesh.keys).union(self.fanout.keys) {
            self.emitGossip(for: topic, into: &outbox)
        }

        /// Shift our message cache window
        self.messageCache.shift()

        /// Return the outbox
        return outbox
    }

    /// Each expired promise adds to its peer's behavioural penalty
    private mutating func applyBrokenPromisePenalties(now: Instant) {
        guard !self.promises.isEmpty else { return }
        var broken: [PeerID: Int] = [:]
        for (id, promisers) in self.promises {
            let expired = promisers.filter { $0.value <= now }
            guard !expired.isEmpty else { continue }
            for peer in expired.keys { broken[peer, default: 0] += 1 }
            let remaining = promisers.filter { $0.value > now }
            self.promises[id] = remaining.isEmpty ? nil : remaining
        }
        for (peer, count) in broken {
            self.scores?.addPenalty(peer, count: count)
        }
    }

    /// Keeps the topic's mesh between `D_lo` and `D_hi`, with at least `D_out` outbound peers, and (when scoring) free of
    /// negative scoring peers
    private mutating func maintainMesh(for topic: String, now: Instant, into outbox: inout Outbox) {
        let degree = self.parameters.meshDegree

        /// Prune the peers with a negative score (without PX)
        for peer in self.mesh[topic] ?? [] where self.score(of: peer) < 0 {
            self.prune(peer, from: topic, peerExchange: false, now: now, into: &outbox)
        }
        guard let mesh = self.mesh[topic] else { return }

        if mesh.count < self.parameters.meshDegreeLow {
            /// We need more peers, graft `D - |mesh|` random topic peers
            for peer in self.graftCandidates(for: topic, now: now).shuffled().prefix(degree - mesh.count) {
                self.addToMesh(peer, topic: topic, now: now)
                outbox.graft(topic, to: peer)
            }
        } else if mesh.count > self.parameters.meshDegreeHigh {
            for peer in self.oversubscriptionPrunes(mesh) {
                self.prune(peer, from: topic, peerExchange: true, now: now, into: &outbox)
            }
        }

        /// Make sure we have at least `D_out` outbound peers, grafting outbound peers if we don't
        if let current = self.mesh[topic], current.count >= self.parameters.meshDegreeLow {
            let outbound = current.filter(self.isOutbound).count
            if outbound < self.parameters.outboundDegree {
                let candidates = self.graftCandidates(for: topic, now: now).filter(self.isOutbound)
                for peer in candidates.shuffled().prefix(self.parameters.outboundDegree - outbound) {
                    self.addToMesh(peer, topic: topic, now: now)
                    outbox.graft(topic, to: peer)
                }
            }
        }

        /// Opportunistic grafting, every `opportunisticGraftTicks`, if the median score of our mesh has dropped below our
        /// opportunistic graft threshold, graft a few peers that score above the median
        if let thresholds = self.parameters.scoring?.thresholds,
            self.ticks % self.parameters.opportunisticGraftTicks == 0,
            let current = self.mesh[topic], current.count > 1
        {
            let scores = current.map(self.score(of:)).sorted()
            let median = scores[scores.count / 2]
            if median < thresholds.opportunisticGraftThreshold {
                let candidates = self.graftCandidates(for: topic, now: now).filter { self.score(of: $0) > median }
                for peer in candidates.shuffled().prefix(self.parameters.opportunisticGraftPeers) {
                    self.addToMesh(peer, topic: topic, now: now)
                    outbox.graft(topic, to: peer)
                }
            }
        }
    }

    /// Selects the peers to prune from an oversubscribed mesh. We keep `D` peers: the `D_score` highest scoring peers,
    /// plus a random selection of the rest, including at least `D_out` outbound peers (if we have that many).
    private func oversubscriptionPrunes(_ mesh: Set<PeerID>) -> [PeerID] {
        let degree = self.parameters.meshDegree
        let retained = min(self.parameters.meshDegreeScore, degree)
        /// shuffle first, so peers with equal scores are selected at random
        let ranked = mesh.shuffled().sorted { self.score(of: $0) > self.score(of: $1) }
        let best = Array(ranked.prefix(retained))
        var random = Array(ranked.dropFirst(retained).shuffled())

        /// the random part of the selection, `random[..<(degree - retained)]`, must top us up to `D_out` outbound peers
        let keptOutbound = (best + random.prefix(degree - retained)).filter(self.isOutbound).count
        var needed = self.parameters.outboundDegree - keptOutbound
        var index = degree - retained
        while needed > 0, index < random.count {
            guard self.isOutbound(random[index]) else {
                index += 1
                continue
            }
            /// swap the outbound peer into the random selection, replacing an inbound peer
            guard let slot = random.prefix(degree - retained).lastIndex(where: { !self.isOutbound($0) }) else { break }
            random.swapAt(slot, index)
            needed -= 1
            index += 1
        }
        return Array(random.dropFirst(degree - retained))
    }

    /// Advertises the topic's recent messages to `max(D_lazy, gossip_factor * |eligible peers|)` random peers outside of the
    /// topic's mesh / fanout (above our gossip threshold), in IHAVEs of at most `maxIHaveLength` message IDs
    private func emitGossip(for topic: String, into outbox: inout Outbox) {
        var ids = self.messageCache.gossipIDs(for: topic)
        guard !ids.isEmpty else { return }
        if ids.count > self.parameters.maxIHaveLength {
            ids = Array(ids.shuffled().prefix(self.parameters.maxIHaveLength))
        }

        /// the peers that already receive full messages for this topic don't need gossip
        let fullPeers = self.mesh[topic] ?? self.fanout[topic] ?? []
        let candidates = self.gossipPeers(subscribedTo: topic).subtracting(fullPeers)
            .filter { self.scoreMeets($0, \.gossipThreshold) }
        let adaptive = Int(self.parameters.gossipFactor * Double(candidates.count))
        let target = max(self.parameters.gossipDegree, adaptive)

        let iHave = RPC.ControlIHave.with {
            $0.topicID = topic
            $0.messageIds = ids
        }
        for peer in candidates.shuffled().prefix(target) {
            /// send the iHave control frame
            outbox.send(control: .with { $0.ihave = [iHave] }, to: peer)
        }
    }
}
