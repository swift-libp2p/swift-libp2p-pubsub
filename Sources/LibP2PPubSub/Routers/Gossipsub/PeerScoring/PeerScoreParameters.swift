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

/// Enables GossipSub v1.1 peer scoring ([spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#peer-scoring)).
///
/// Every peer is given a score
/// - based on how it behaves in our topics (P1 to P4),
/// - an application defined score (P5),
/// - how many peers share its IP address (P6),
/// - and how often it misbehaves at the protocol level (P7).
///
/// Peers with a negative score are pruned from our meshes and never grafted, and the thresholds progressively stop us
/// gossiping with, publishing to, and finally accepting anything from, low scoring peers.
///
/// - Important: Scores are local, but the parameters should be tuned for your network's topics (message rates, sizes, etc.).
///   The defaults are only a starting point.
public struct GossipSubScoring: Sendable {
    public let parameters: PeerScoreParameters
    public let thresholds: PeerScoreThresholds

    /// - Throws: A ``PeerScoreParameterError`` if the parameters or thresholds are invalid.
    public init(parameters: PeerScoreParameters, thresholds: PeerScoreThresholds = .init()) throws {
        try parameters.validate()
        try thresholds.validate()
        self.parameters = parameters
        self.thresholds = thresholds
    }
}

/// Describes an invalid peer scoring parameter
public struct PeerScoreParameterError: Error, CustomStringConvertible, Equatable {
    public let description: String

    init(_ description: String) {
        self.description = description
    }
}

/// The score thresholds that gate how we interact with peers
///
/// They must satisfy `graylistThreshold <= publishThreshold <= gossipThreshold <= 0`, while `acceptPXThreshold` and
/// `opportunisticGraftThreshold` must be non-negative.
public struct PeerScoreThresholds: Sendable, Equatable {
    
    /// Below this score we don't gossip to the peer, and ignore its gossip (IHAVE / IWANT)
    public var gossipThreshold: Double

    /// Below this score we don't publish our own messages to the peer (flood publishing or fanout)
    public var publishThreshold: Double

    /// Below this score we ignore every RPC the peer sends us
    public var graylistThreshold: Double

    /// Below this score we ignore peer suggestions via PX (when peer exchange is enabled)
    public var acceptPXThreshold: Double

    /// When the median score of a topic's mesh drops below this, we opportunistically graft higher scoring peers
    public var opportunisticGraftThreshold: Double

    public init(
        gossipThreshold: Double = -10,
        publishThreshold: Double = -50,
        graylistThreshold: Double = -80,
        acceptPXThreshold: Double = 10,
        opportunisticGraftThreshold: Double = 1
    ) {
        self.gossipThreshold = gossipThreshold
        self.publishThreshold = publishThreshold
        self.graylistThreshold = graylistThreshold
        self.acceptPXThreshold = acceptPXThreshold
        self.opportunisticGraftThreshold = opportunisticGraftThreshold
    }

    func validate() throws {
        guard [gossipThreshold, publishThreshold, graylistThreshold, acceptPXThreshold, opportunisticGraftThreshold]
            .allSatisfy(\.isFinite)
        else { throw PeerScoreParameterError("Score thresholds must be finite") }
        guard gossipThreshold <= 0 else { throw PeerScoreParameterError("gossipThreshold must be <= 0") }
        guard publishThreshold <= gossipThreshold else {
            throw PeerScoreParameterError("publishThreshold must be <= gossipThreshold")
        }
        guard graylistThreshold <= publishThreshold else {
            throw PeerScoreParameterError("graylistThreshold must be <= publishThreshold")
        }
        guard acceptPXThreshold >= 0 else { throw PeerScoreParameterError("acceptPXThreshold must be >= 0") }
        guard opportunisticGraftThreshold >= 0 else {
            throw PeerScoreParameterError("opportunisticGraftThreshold must be >= 0")
        }
    }
}

/// The parameters of the peer score function
///
/// ```
/// score = min(Σ topicWeight * topicScore, topicScoreCap)
///       + P5 * appSpecificWeight
///       + P6 * ipColocationFactorWeight
///       + P7 * behaviourPenaltyWeight
/// ```
public struct PeerScoreParameters: Sendable {
    
    /// The scoring parameters for each scored topic. Topics without parameters don't contribute to a peer's score.
    public var topics: [String: TopicScoreParameters]

    /// Caps the (positive) contribution of all topics to a peer's score. `0` means uncapped.
    public var topicScoreCap: Double

    /// P5, an application defined score for each peer
    public var appSpecificScore: @Sendable (PeerID) -> Double
    public var appSpecificWeight: Double

    /// P6, penalizes peers sharing an IP address with more than `ipColocationFactorThreshold` other peers
    /// (by the square of the surplus). The weight must be negative, or `0` to disable P6.
    public var ipColocationFactorWeight: Double
    public var ipColocationFactorThreshold: Int
    /// IP addresses that are never penalized (ex: a local network's NAT)
    public var ipColocationFactorWhitelist: Set<String>

    /// P7, penalizes protocol misbehaviour (broken IWANT promises, grafting while backed off), by the square of the
    /// penalty above `behaviourPenaltyThreshold`. The weight must be negative, or `0` to disable P7.
    public var behaviourPenaltyWeight: Double
    public var behaviourPenaltyThreshold: Double
    public var behaviourPenaltyDecay: Double

    /// How often the score counters decay
    public var decayInterval: Duration
    /// Counters that decay below this value are reset to zero
    public var decayToZero: Double
    /// How long we remember the score of a peer that disconnected (so it can't reset a bad score by reconnecting)
    public var retainScore: Duration

    public init(
        topics: [String: TopicScoreParameters] = [:],
        topicScoreCap: Double = 0,
        appSpecificScore: @escaping @Sendable (PeerID) -> Double = { _ in 0 },
        appSpecificWeight: Double = 1,
        ipColocationFactorWeight: Double = 0,
        ipColocationFactorThreshold: Int = 10,
        ipColocationFactorWhitelist: Set<String> = [],
        behaviourPenaltyWeight: Double = -1,
        behaviourPenaltyThreshold: Double = 0,
        behaviourPenaltyDecay: Double = 0.99,
        decayInterval: Duration = .seconds(1),
        decayToZero: Double = 0.01,
        retainScore: Duration = .seconds(600)
    ) {
        self.topics = topics
        self.topicScoreCap = topicScoreCap
        self.appSpecificScore = appSpecificScore
        self.appSpecificWeight = appSpecificWeight
        self.ipColocationFactorWeight = ipColocationFactorWeight
        self.ipColocationFactorThreshold = ipColocationFactorThreshold
        self.ipColocationFactorWhitelist = ipColocationFactorWhitelist
        self.behaviourPenaltyWeight = behaviourPenaltyWeight
        self.behaviourPenaltyThreshold = behaviourPenaltyThreshold
        self.behaviourPenaltyDecay = behaviourPenaltyDecay
        self.decayInterval = decayInterval
        self.decayToZero = decayToZero
        self.retainScore = retainScore
    }

    func validate() throws {
        for (topic, parameters) in self.topics {
            do { try parameters.validate() } catch let error as PeerScoreParameterError {
                throw PeerScoreParameterError("Topic `\(topic)`: \(error.description)")
            }
        }
        guard topicScoreCap >= 0 else { throw PeerScoreParameterError("topicScoreCap must be >= 0 (or 0 for no cap)") }
        guard ipColocationFactorWeight <= 0 else {
            throw PeerScoreParameterError("ipColocationFactorWeight must be negative (or 0 to disable)")
        }
        if ipColocationFactorWeight != 0 && ipColocationFactorThreshold < 1 {
            throw PeerScoreParameterError("ipColocationFactorThreshold must be at least 1")
        }
        guard behaviourPenaltyWeight <= 0 else {
            throw PeerScoreParameterError("behaviourPenaltyWeight must be negative (or 0 to disable)")
        }
        if behaviourPenaltyWeight != 0 && !(0 < behaviourPenaltyDecay && behaviourPenaltyDecay < 1) {
            throw PeerScoreParameterError("behaviourPenaltyDecay must be between 0 and 1")
        }
        guard behaviourPenaltyThreshold >= 0 else { throw PeerScoreParameterError("behaviourPenaltyThreshold must be >= 0") }
        guard decayInterval >= .seconds(1) else { throw PeerScoreParameterError("decayInterval must be at least 1 second") }
        guard 0 < decayToZero && decayToZero < 1 else { throw PeerScoreParameterError("decayToZero must be between 0 and 1") }
        guard retainScore >= .zero else { throw PeerScoreParameterError("retainScore can't be negative") }
    }
}

/// The parameters of a topic's contribution to a peer's score
///
/// ```
/// topicScore = P1 * timeInMeshWeight
///            + P2 * firstMessageDeliveriesWeight
///            + P3 * meshMessageDeliveriesWeight
///            + P3b * meshFailurePenaltyWeight
///            + P4 * invalidMessageDeliveriesWeight
/// ```
///
/// - Note: P3 and P3b (mesh message delivery rates) depend heavily on a topic's message rate, so they're disabled by default.
public struct TopicScoreParameters: Sendable, Equatable {
    
    /// How much this topic contributes to the peer's score. Must be non-negative.
    public var topicWeight: Double

    /// P1, rewards the time a peer has spent in our mesh, in multiples of `timeInMeshQuantum` (up to `timeInMeshCap`)
    public var timeInMeshWeight: Double
    public var timeInMeshQuantum: Duration
    public var timeInMeshCap: Double

    /// P2, rewards peers that are the first to deliver us valid messages (up to `firstMessageDeliveriesCap`)
    public var firstMessageDeliveriesWeight: Double
    public var firstMessageDeliveriesDecay: Double
    public var firstMessageDeliveriesCap: Double

    /// P3, penalizes mesh peers that deliver fewer than `meshMessageDeliveriesThreshold` messages (first, or within
    /// `meshMessageDeliveriesWindow` of the first), by the square of the deficit. Only applies once a peer has been in our
    /// mesh for `meshMessageDeliveriesActivation`. The weight must be negative, or `0` to disable P3.
    public var meshMessageDeliveriesWeight: Double
    public var meshMessageDeliveriesDecay: Double
    public var meshMessageDeliveriesCap: Double
    public var meshMessageDeliveriesThreshold: Double
    public var meshMessageDeliveriesWindow: Duration
    public var meshMessageDeliveriesActivation: Duration

    /// P3b, a sticky penalty (the P3 deficit squared) applied when a mesh peer with a delivery deficit is pruned.
    /// The weight must be negative, or `0` to disable P3b.
    public var meshFailurePenaltyWeight: Double
    public var meshFailurePenaltyDecay: Double

    /// P4, penalizes invalid messages, by the square of the (decaying) count. The weight must be negative.
    public var invalidMessageDeliveriesWeight: Double
    public var invalidMessageDeliveriesDecay: Double

    public init(
        topicWeight: Double = 1,
        timeInMeshWeight: Double = 0.01,
        timeInMeshQuantum: Duration = .seconds(1),
        timeInMeshCap: Double = 300,
        firstMessageDeliveriesWeight: Double = 1,
        firstMessageDeliveriesDecay: Double = 0.5,
        firstMessageDeliveriesCap: Double = 20,
        meshMessageDeliveriesWeight: Double = 0,
        meshMessageDeliveriesDecay: Double = 0.5,
        meshMessageDeliveriesCap: Double = 20,
        meshMessageDeliveriesThreshold: Double = 1,
        meshMessageDeliveriesWindow: Duration = .milliseconds(10),
        meshMessageDeliveriesActivation: Duration = .seconds(5),
        meshFailurePenaltyWeight: Double = 0,
        meshFailurePenaltyDecay: Double = 0.5,
        invalidMessageDeliveriesWeight: Double = -1,
        invalidMessageDeliveriesDecay: Double = 0.3
    ) {
        self.topicWeight = topicWeight
        self.timeInMeshWeight = timeInMeshWeight
        self.timeInMeshQuantum = timeInMeshQuantum
        self.timeInMeshCap = timeInMeshCap
        self.firstMessageDeliveriesWeight = firstMessageDeliveriesWeight
        self.firstMessageDeliveriesDecay = firstMessageDeliveriesDecay
        self.firstMessageDeliveriesCap = firstMessageDeliveriesCap
        self.meshMessageDeliveriesWeight = meshMessageDeliveriesWeight
        self.meshMessageDeliveriesDecay = meshMessageDeliveriesDecay
        self.meshMessageDeliveriesCap = meshMessageDeliveriesCap
        self.meshMessageDeliveriesThreshold = meshMessageDeliveriesThreshold
        self.meshMessageDeliveriesWindow = meshMessageDeliveriesWindow
        self.meshMessageDeliveriesActivation = meshMessageDeliveriesActivation
        self.meshFailurePenaltyWeight = meshFailurePenaltyWeight
        self.meshFailurePenaltyDecay = meshFailurePenaltyDecay
        self.invalidMessageDeliveriesWeight = invalidMessageDeliveriesWeight
        self.invalidMessageDeliveriesDecay = invalidMessageDeliveriesDecay
    }

    func validate() throws {
        func isDecay(_ value: Double) -> Bool { 0 < value && value < 1 }

        guard topicWeight >= 0 else { throw PeerScoreParameterError("topicWeight must be >= 0") }

        guard timeInMeshWeight >= 0 else { throw PeerScoreParameterError("timeInMeshWeight must be >= 0") }
        if timeInMeshWeight != 0 {
            guard timeInMeshQuantum > .zero else { throw PeerScoreParameterError("timeInMeshQuantum must be positive") }
            guard timeInMeshCap > 0 else { throw PeerScoreParameterError("timeInMeshCap must be positive") }
        }

        guard firstMessageDeliveriesWeight >= 0 else {
            throw PeerScoreParameterError("firstMessageDeliveriesWeight must be >= 0")
        }
        if firstMessageDeliveriesWeight != 0 {
            guard isDecay(firstMessageDeliveriesDecay) else {
                throw PeerScoreParameterError("firstMessageDeliveriesDecay must be between 0 and 1")
            }
            guard firstMessageDeliveriesCap > 0 else {
                throw PeerScoreParameterError("firstMessageDeliveriesCap must be positive")
            }
        }

        guard meshMessageDeliveriesWeight <= 0 else {
            throw PeerScoreParameterError("meshMessageDeliveriesWeight must be negative (or 0 to disable)")
        }
        if meshMessageDeliveriesWeight != 0 {
            guard isDecay(meshMessageDeliveriesDecay) else {
                throw PeerScoreParameterError("meshMessageDeliveriesDecay must be between 0 and 1")
            }
            guard meshMessageDeliveriesCap > 0 else { throw PeerScoreParameterError("meshMessageDeliveriesCap must be positive") }
            guard meshMessageDeliveriesThreshold > 0 else {
                throw PeerScoreParameterError("meshMessageDeliveriesThreshold must be positive")
            }
            guard meshMessageDeliveriesWindow >= .zero else {
                throw PeerScoreParameterError("meshMessageDeliveriesWindow can't be negative")
            }
            guard meshMessageDeliveriesActivation >= .seconds(1) else {
                throw PeerScoreParameterError("meshMessageDeliveriesActivation must be at least 1 second")
            }
        }

        guard meshFailurePenaltyWeight <= 0 else {
            throw PeerScoreParameterError("meshFailurePenaltyWeight must be negative (or 0 to disable)")
        }
        if meshFailurePenaltyWeight != 0 && !isDecay(meshFailurePenaltyDecay) {
            throw PeerScoreParameterError("meshFailurePenaltyDecay must be between 0 and 1")
        }

        guard invalidMessageDeliveriesWeight <= 0 else {
            throw PeerScoreParameterError("invalidMessageDeliveriesWeight must be negative (or 0 to disable)")
        }
        if invalidMessageDeliveriesWeight != 0 && !isDecay(invalidMessageDeliveriesDecay) {
            throw PeerScoreParameterError("invalidMessageDeliveriesDecay must be between 0 and 1")
        }
    }
}
