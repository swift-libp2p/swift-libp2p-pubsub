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

/// How a message's ID is derived.
///
/// Message IDs are used to uniquely define messages so we can...
/// - drop / ignore duplicate messages
/// - advertise (IHAVE) and request (IWANT) messages in gossipsub
///
/// The hashed strategies produce the same IDs as swift-libp2p-core's equivalent `PubSub.MessageIDFunction`s.
///
/// - Important: Every peer on a given topic must derive IDs the same way.
public enum MessageIDStrategy: Sendable {

    /// The spec's default, `from` followed by `seqno`.
    ///
    /// - Note: Only unique for signed (StrictSign) messages.
    case fromAndSequenceNumber

    /// SHA-256 of `seqno` followed by `from` (core's `.hashSequenceNumberAndFromFields`)
    case hashedSequenceNumberAndFrom

    /// SHA-256 of `seqno`, `from`, `data` and the topic (core's `.hashEverything`)
    case hashedMessage

    /// SHA-256 of `data` (core's `.contentHash`). The usual choice for StrictNoSign topics.
    case contentHash

    /// A custom ID function
    case custom(@Sendable (PubSubMessage) -> Data)

    private static let hashedSequenceNumberAndFromID = PubSub.MessageIDFunction.hashSequenceNumberAndFromFields
        .messageIDFunction
    private static let hashedMessageID = PubSub.MessageIDFunction.hashEverything.messageIDFunction
    private static let contentHashID = PubSub.MessageIDFunction.contentHash.messageIDFunction

    /// Computes the ID of `message`
    public func id(for message: PubSubMessage) -> Data {
        switch self {
        case .fromAndSequenceNumber:
            return message.from + message.seqno
        case .hashedSequenceNumberAndFrom:
            return Self.hashedSequenceNumberAndFromID(message)
        case .hashedMessage:
            return Self.hashedMessageID(message)
        case .contentHash:
            return Self.contentHashID(message)
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
