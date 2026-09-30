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

import Crypto
import LibP2P

/// How a message's ID is derived.
///
/// Message IDs are used to uniquely define messages so we can...
/// - drop / ignore duplicate messages
/// - advertise (IHAVE) and request (IWANT) messages in gossipsub
///
/// - Important: Every peer on a given topic must derive IDs the same way.
public enum MessageIDStrategy: Sendable {

    /// The spec's default, `from` followed by `seqno`.
    ///
    /// - Note: Only unique for signed (StrictSign) messages.
    case fromAndSequenceNumber

    /// SHA-256 of `seqno` followed by `from`
    case hashedSequenceNumberAndFrom

    /// SHA-256 of `seqno`, `from`, `data` and the topic
    case hashedMessage

    /// SHA-256 of `data`. The usual choice for StrictNoSign topics.
    case contentHash

    /// A custom ID function
    case custom(@Sendable (PubSubMessage) -> Data)

    /// Computes the ID of `message`
    public func id(for message: PubSubMessage) -> Data {
        switch self {
        case .fromAndSequenceNumber:
            return message.from + message.seqno
        case .hashedSequenceNumberAndFrom:
            return Data(SHA256.hash(data: message.seqno + message.from))
        case .hashedMessage:
            var hasher = SHA256()
            hasher.update(data: message.seqno)
            hasher.update(data: message.from)
            hasher.update(data: message.data)
            for topic in message.topicIds { hasher.update(data: Data(topic.utf8)) }
            return Data(hasher.finalize())
        case .contentHash:
            return Data(SHA256.hash(data: message.data))
        case .custom(let function):
            return function(message)
        }
    }

    /// Whether this strategy relies solely on the `from` and `seqno` fields
    var dependsOnAuthorship: Bool {
        switch self {
        case .fromAndSequenceNumber, .hashedSequenceNumberAndFrom: return true
        case .hashedMessage, .contentHash, .custom: return false
        }
    }

    /// The default strategy for a signature policy
    static func `default`(for policy: PubSub.SignaturePolicy) -> MessageIDStrategy {
        switch policy {
        case .strictSign: return .fromAndSequenceNumber
        case .strictNoSign: return .contentHash
        }
    }
}
