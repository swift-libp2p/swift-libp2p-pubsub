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

/// Why a message was rejected, which determines how the peers that delivered it are scored
enum MessageRejection: Sendable, Equatable {
    /// The message's signature was missing, invalid or unexpected (penalizes the peer that sent it to us)
    case invalidSignature
    /// A peer sent us one of our own messages (penalizes the peer that sent it to us)
    case selfOrigin
    /// A validator rejected the message (penalizes every peer that delivered it)
    case invalid
    /// A validator ignored the message (nobody is penalized)
    case ignored
    /// A validator throttled the message (nobody is penalized)
    case throttled
}

/// GossipSub v1.1's peer score ([spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#the-score-function)),
/// modelled on go-libp2p-pubsub's `peerScore`.
///
/// Tracks, for each peer and scored topic:
/// - P1: how long it's been in our mesh
/// - P2: how many messages it was the first to deliver
/// - P3: whether (as a mesh peer) it delivers enough messages
///     - P3b: a sticky penalty when it's pruned with a deficit
/// - P4: how many invalid messages it delivered
/// and for each peer:
/// - P5: the application's score
/// - P6: how many peers share its IP addresses
/// - P7: its protocol misbehaviour.
///
/// The counters decay every `decayInterval`, and the stats of disconnected peers with a non-positive score are retained for
/// `retainScore`, so a peer can't reset a bad score by reconnecting.
struct PeerScore {
    typealias Instant = ContinuousClock.Instant

    struct TopicStats {
        var inMesh = false
        var graftTime: Instant? = nil
        var meshTime: Duration = .zero
        var firstMessageDeliveries: Double = 0
        var meshMessageDeliveries: Double = 0
        var meshMessageDeliveriesActive = false
        var meshFailurePenalty: Double = 0
        var invalidMessageDeliveries: Double = 0
    }

    struct PeerStats {
        var connected = true
        /// When we forget a disconnected peer's stats
        var expires: Instant? = nil
        var topics: [String: TopicStats] = [:]
        var ips: Set<String> = []
        var behaviourPenalty: Double = 0
    }

    enum DeliveryStatus {
        /// The message is being validated
        case unknown
        case valid
        case invalid
        case ignored
        case throttled
    }

    /// What we know about a recently seen message, and the peers that delivered it while it was being validated
    struct DeliveryRecord {
        let topic: String
        var status: DeliveryStatus = .unknown
        var validated: Instant? = nil
        var peers: Set<PeerID> = []
    }

    let parameters: PeerScoreParameters

    /// How long we remember delivery records for (go-libp2p-pubsub's `TimeCacheDuration`)
    let deliveryRecordTTL: Duration

    private(set) var peers: [PeerID: PeerStats] = [:]
    private var peersByIP: [String: Set<PeerID>] = [:]
    private(set) var deliveries: [Data: DeliveryRecord] = [:]
    private var deliveryExpiries: [(id: Data, expiry: Instant)] = []
    private var deliveryExpiriesHead = 0
    private var lastDecay: Instant? = nil

    init(parameters: PeerScoreParameters, deliveryRecordTTL: Duration = .seconds(120)) {
        self.parameters = parameters
        self.deliveryRecordTTL = deliveryRecordTTL
    }

    // MARK: - Score

    func score(of peer: PeerID) -> Double {
        guard let stats = self.peers[peer] else { return 0 }

        var score: Double = 0
        for (topic, topicStats) in stats.topics {
            guard let parameters = self.parameters.topics[topic] else { continue }
            var topicScore: Double = 0

            /// P1: time in mesh
            if topicStats.inMesh, parameters.timeInMeshQuantum > .zero {
                let quanta = (topicStats.meshTime / parameters.timeInMeshQuantum).rounded(.down)
                topicScore += min(quanta, parameters.timeInMeshCap) * parameters.timeInMeshWeight
            }

            /// P2: first message deliveries
            topicScore += topicStats.firstMessageDeliveries * parameters.firstMessageDeliveriesWeight

            /// P3: mesh message delivery deficit
            if topicStats.meshMessageDeliveriesActive,
                topicStats.meshMessageDeliveries < parameters.meshMessageDeliveriesThreshold
            {
                let deficit = parameters.meshMessageDeliveriesThreshold - topicStats.meshMessageDeliveries
                topicScore += deficit * deficit * parameters.meshMessageDeliveriesWeight
            }

            /// P3b: mesh failure penalty
            topicScore += topicStats.meshFailurePenalty * parameters.meshFailurePenaltyWeight

            /// P4: invalid messages
            topicScore +=
                topicStats.invalidMessageDeliveries * topicStats.invalidMessageDeliveries
                * parameters.invalidMessageDeliveriesWeight

            score += topicScore * parameters.topicWeight
        }
        if self.parameters.topicScoreCap > 0 { score = min(score, self.parameters.topicScoreCap) }

        /// P5: application specific score
        score += self.parameters.appSpecificScore(peer) * self.parameters.appSpecificWeight

        /// P6: IP colocation
        score += self.ipColocationFactor(of: stats) * self.parameters.ipColocationFactorWeight

        /// P7: behavioural penalty
        if stats.behaviourPenalty > self.parameters.behaviourPenaltyThreshold {
            let excess = stats.behaviourPenalty - self.parameters.behaviourPenaltyThreshold
            score += excess * excess * self.parameters.behaviourPenaltyWeight
        }
        return score
    }

    /// The sum of the squared surplus of peers (above the threshold) sharing each of the peer's (non whitelisted) IPs
    private func ipColocationFactor(of stats: PeerStats) -> Double {
        var factor: Double = 0
        for ip in stats.ips where !self.parameters.ipColocationFactorWhitelist.contains(ip) {
            let surplus = (self.peersByIP[ip]?.count ?? 0) - self.parameters.ipColocationFactorThreshold
            if surplus > 0 { factor += Double(surplus * surplus) }
        }
        return factor
    }

    // MARK: - Peers

    /// A peer connected (or reconnected, restoring any retained stats)
    mutating func addPeer(_ peer: PeerID, ip: String?) {
        var stats = self.peers[peer] ?? PeerStats()
        stats.connected = true
        stats.expires = nil
        self.removeIPs(of: peer, stats.ips)
        stats.ips = ip.map { [$0] } ?? []
        for ip in stats.ips { self.peersByIP[ip, default: []].insert(peer) }
        self.peers[peer] = stats
    }

    /// A peer disconnected. Peers with a positive score are forgotten, the rest are retained for `retainScore`
    /// (with their first delivery counts reset, and the mesh failure penalty applied to the meshes they were in).
    mutating func removePeer(_ peer: PeerID, now: Instant) {
        guard var stats = self.peers[peer] else { return }
        self.removeIPs(of: peer, stats.ips)
        stats.ips = []
        guard self.score(of: peer) <= 0 else {
            self.peers.removeValue(forKey: peer)
            return
        }
        for topic in stats.topics.keys {
            guard var topicStats = stats.topics[topic] else { continue }
            topicStats.firstMessageDeliveries = 0
            if topicStats.inMesh {
                Self.applyMeshFailurePenalty(to: &topicStats, parameters: self.parameters.topics[topic])
                topicStats.meshMessageDeliveriesActive = false
                topicStats.inMesh = false
            }
            stats.topics[topic] = topicStats
        }
        stats.connected = false
        stats.expires = now + self.parameters.retainScore
        self.peers[peer] = stats
    }

    private mutating func removeIPs(of peer: PeerID, _ ips: Set<String>) {
        for ip in ips {
            self.peersByIP[ip]?.remove(peer)
            if self.peersByIP[ip]?.isEmpty == true { self.peersByIP.removeValue(forKey: ip) }
        }
    }

    /// Adds `count` to the peer's behavioural penalty (P7)
    mutating func addPenalty(_ peer: PeerID, count: Int = 1) {
        self.peers[peer, default: PeerStats()].behaviourPenalty += Double(count)
    }

    // MARK: - Meshes

    mutating func graft(_ peer: PeerID, topic: String, now: Instant) {
        self.updateTopicStats(of: peer, topic: topic) { stats in
            stats.inMesh = true
            stats.graftTime = now
            stats.meshTime = .zero
            stats.meshMessageDeliveriesActive = false
        }
    }

    mutating func prune(_ peer: PeerID, topic: String) {
        let parameters = self.parameters.topics[topic]
        self.updateTopicStats(of: peer, topic: topic) { stats in
            guard stats.inMesh else { return }
            Self.applyMeshFailurePenalty(to: &stats, parameters: parameters)
            /// once out of the mesh, the P3 deficit no longer applies (the sticky P3b penalty does)
            stats.meshMessageDeliveriesActive = false
            stats.inMesh = false
        }
    }

    /// P3b, a peer leaving our mesh with a mesh delivery deficit keeps (the square of) that deficit as a sticky penalty
    private static func applyMeshFailurePenalty(to stats: inout TopicStats, parameters: TopicScoreParameters?) {
        guard let parameters, stats.meshMessageDeliveriesActive,
            stats.meshMessageDeliveries < parameters.meshMessageDeliveriesThreshold
        else { return }
        let deficit = parameters.meshMessageDeliveriesThreshold - stats.meshMessageDeliveries
        stats.meshFailurePenalty += deficit * deficit
    }

    /// Only topics with scoring parameters are tracked
    private mutating func updateTopicStats(of peer: PeerID, topic: String, _ update: (inout TopicStats) -> Void) {
        guard self.parameters.topics[topic] != nil else { return }
        var stats = self.peers[peer] ?? PeerStats()
        var topicStats = stats.topics[topic] ?? TopicStats()
        update(&topicStats)
        stats.topics[topic] = topicStats
        self.peers[peer] = stats
    }

    // MARK: - Message deliveries

    /// A new message (first delivered by `source`) is being validated
    mutating func validationStarted(_ id: Data, topic: String, now: Instant) {
        guard self.deliveries[id] == nil else { return }
        self.deliveries[id] = DeliveryRecord(topic: topic)
        self.deliveryExpiries.append((id, now + self.deliveryRecordTTL))
    }

    /// A message passed validation. Its source gets credit for the first delivery, and the mesh peers that delivered it while
    /// it was being validated get credit for near-first deliveries.
    mutating func delivered(_ id: Data, topic: String, from source: PeerID, now: Instant) {
        self.markFirstMessageDelivery(by: source, topic: topic)
        var record = self.deliveries[id] ?? DeliveryRecord(topic: topic)
        guard record.status == .unknown else { return }
        record.status = .valid
        record.validated = now
        for peer in record.peers where peer != source {
            self.markDuplicateMessageDelivery(by: peer, topic: topic, validated: now, now: now)
        }
        record.peers = []
        self.deliveries[id] = record
    }

    /// A message was rejected
    mutating func rejected(_ id: Data, topic: String, from source: PeerID, reason: MessageRejection, now: Instant) {
        switch reason {
        case .invalidSignature, .selfOrigin:
            /// We don't track these messages, but the peer that sent them is clearly misbehaving
            self.markInvalidMessageDelivery(by: source, topic: topic)
            return
        case .invalid, .ignored, .throttled:
            break
        }

        var record = self.deliveries[id] ?? DeliveryRecord(topic: topic)
        guard record.status == .unknown else { return }
        switch reason {
        case .ignored:
            record.status = .ignored
        case .throttled:
            record.status = .throttled
        default:
            record.status = .invalid
            self.markInvalidMessageDelivery(by: source, topic: topic)
            for peer in record.peers { self.markInvalidMessageDelivery(by: peer, topic: topic) }
        }
        record.peers = []
        self.deliveries[id] = record
    }

    /// A peer delivered a message we've already seen (or are validating)
    mutating func duplicate(_ id: Data, topic: String, from peer: PeerID, now: Instant) {
        guard var record = self.deliveries[id] else { return }
        switch record.status {
        case .unknown:
            /// Still being validated, the peer is scored once we know the message's fate
            record.peers.insert(peer)
            self.deliveries[id] = record
        case .valid:
            record.peers.insert(peer)
            self.deliveries[id] = record
            self.markDuplicateMessageDelivery(by: peer, topic: topic, validated: record.validated, now: now)
        case .invalid:
            self.markInvalidMessageDelivery(by: peer, topic: topic)
        case .ignored, .throttled:
            break
        }
    }

    private mutating func markFirstMessageDelivery(by peer: PeerID, topic: String) {
        guard let parameters = self.parameters.topics[topic] else { return }
        self.updateTopicStats(of: peer, topic: topic) { stats in
            stats.firstMessageDeliveries = min(stats.firstMessageDeliveries + 1, parameters.firstMessageDeliveriesCap)
            if stats.inMesh {
                stats.meshMessageDeliveries = min(stats.meshMessageDeliveries + 1, parameters.meshMessageDeliveriesCap)
            }
        }
    }

    /// Mesh peers get credit for deliveries within `meshMessageDeliveriesWindow` of the message being validated
    private mutating func markDuplicateMessageDelivery(
        by peer: PeerID,
        topic: String,
        validated: Instant?,
        now: Instant
    ) {
        guard let parameters = self.parameters.topics[topic] else { return }
        if let validated, validated.duration(to: now) > parameters.meshMessageDeliveriesWindow { return }
        self.updateTopicStats(of: peer, topic: topic) { stats in
            guard stats.inMesh else { return }
            stats.meshMessageDeliveries = min(stats.meshMessageDeliveries + 1, parameters.meshMessageDeliveriesCap)
        }
    }

    private mutating func markInvalidMessageDelivery(by peer: PeerID, topic: String) {
        self.updateTopicStats(of: peer, topic: topic) { $0.invalidMessageDeliveries += 1 }
    }

    // MARK: - Decay

    /// Decays every counter (at most once per `decayInterval`), updates mesh times, and forgets expired peers and delivery records
    mutating func refresh(now: Instant) {
        self.expireDeliveries(now: now)
        if let last = self.lastDecay, last.duration(to: now) < self.parameters.decayInterval { return }
        self.lastDecay = now

        let decayToZero = self.parameters.decayToZero
        func decay(_ value: inout Double, by factor: Double) {
            value *= factor
            if abs(value) < decayToZero { value = 0 }
        }

        for (peer, var stats) in self.peers {
            guard stats.connected else {
                if let expires = stats.expires, expires <= now { self.peers.removeValue(forKey: peer) }
                continue
            }
            for (topic, var topicStats) in stats.topics {
                guard let parameters = self.parameters.topics[topic] else { continue }
                decay(&topicStats.firstMessageDeliveries, by: parameters.firstMessageDeliveriesDecay)
                decay(&topicStats.meshMessageDeliveries, by: parameters.meshMessageDeliveriesDecay)
                decay(&topicStats.meshFailurePenalty, by: parameters.meshFailurePenaltyDecay)
                decay(&topicStats.invalidMessageDeliveries, by: parameters.invalidMessageDeliveriesDecay)
                if topicStats.inMesh, let graftTime = topicStats.graftTime {
                    topicStats.meshTime = graftTime.duration(to: now)
                    if topicStats.meshTime > parameters.meshMessageDeliveriesActivation {
                        topicStats.meshMessageDeliveriesActive = true
                    }
                }
                stats.topics[topic] = topicStats
            }
            decay(&stats.behaviourPenalty, by: self.parameters.behaviourPenaltyDecay)
            self.peers[peer] = stats
        }
    }

    private mutating func expireDeliveries(now: Instant) {
        while self.deliveryExpiriesHead < self.deliveryExpiries.count,
            self.deliveryExpiries[self.deliveryExpiriesHead].expiry <= now
        {
            self.deliveries.removeValue(forKey: self.deliveryExpiries[self.deliveryExpiriesHead].id)
            self.deliveryExpiriesHead += 1
        }
        if self.deliveryExpiriesHead > 1024 && self.deliveryExpiriesHead * 2 > self.deliveryExpiries.count {
            self.deliveryExpiries.removeFirst(self.deliveryExpiriesHead)
            self.deliveryExpiriesHead = 0
        }
    }
}
