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

/// Message signing and signature policy enforcement
///
/// [Spec](https://github.com/libp2p/specs/blob/master/pubsub/README.md#message-signing)
enum MessageSigning {

    /// Prepended to the marshalled message before signing
    static let signaturePrefix = Data("libp2p-pubsub:".utf8)

    /// The reasons a message can fail its topic's signature policy
    enum Violation: Error, Equatable {

        /// The spec defines a single topic per message
        case invalidTopicCount(Int)

        /// A StrictSign message is missing its `signature`, `from` or `seqno`
        case missingSignature

        /// A StrictSign message's signature doesn't verify
        case invalidSignature

        /// A StrictNoSign message contains `from`, `seqno`, `signature` or `key`
        case unexpectedAuthorship
    }

    /// Prepares one of our own messages for publishing
    ///
    /// - StrictSign: sets `signature` over `signaturePrefix + marshal(message without signature & key)`.
    ///   The `key` field is only included when the public key can't be extracted from our PeerID (ex: RSA).
    /// - StrictNoSign: strips the `from`, `seqno`, `signature` and `key` fields.
    ///
    /// - Note: These are proto2 `optional` fields. Assigning an empty `Data()` would mark them as present (and serialize them),
    ///   which changes the signed bytes, so they're explicitly cleared instead.
    static func prepare(_ message: RPC.Message, policy: PubSub.SignaturePolicy, signer: PeerID) throws -> RPC.Message {
        var prepared = message
        prepared.clearSignature()
        prepared.clearKey()
        switch policy {
        case .strictSign:
            let bytes = try Self.signaturePrefix + prepared.serializedData()
            prepared.signature = try signer.signature(for: bytes)
            if !Self.hasInlinedPublicKey(signer) {
                prepared.key = try Data(signer.marshalPublicKey())
            }
        case .strictNoSign:
            prepared.clearFrom()
            prepared.clearSeqno()
        }
        return prepared
    }

    /// Checks that an inbound message conforms to its topic's signature policy (verifying its signature when required).
    static func check(_ message: RPC.Message, against policy: PubSub.SignaturePolicy) -> Violation? {
        guard message.topicIds.count == 1 else { return .invalidTopicCount(message.topicIds.count) }
        switch policy {
        case .strictNoSign:
            guard !message.hasFrom, !message.hasSeqno, !message.hasSignature, !message.hasKey else {
                return .unexpectedAuthorship
            }
            return nil
        case .strictSign:
            guard !message.signature.isEmpty, !message.from.isEmpty, !message.seqno.isEmpty else {
                return .missingSignature
            }
            return Self.hasValidSignature(message) ? nil : .invalidSignature
        }
    }

    /// Verifies a signed message
    ///
    /// - If the `key` field is present, it must belong to the `from` PeerID.
    /// - If the `key` field is absent, the public key must be extractable from the `from` PeerID (identity multihash PeerIDs).
    /// - The signature covers the marshalled message with `signature` and `key` cleared. Like go-libp2p-pubsub we clear those
    ///   fields on a copy (rather than rebuilding the message) so field presence and unknown fields are preserved.
    static func hasValidSignature(_ message: RPC.Message) -> Bool {
        guard let author = try? PeerID(fromBytesID: Array(message.from)) else { return false }

        let signer: PeerID
        if message.key.isEmpty {
            guard author.type != .idOnly else { return false }
            signer = author
        } else {
            guard let key = try? PeerID(marshaledPublicKey: message.key), key == author else { return false }
            signer = key
        }

        var unsigned = message
        unsigned.clearSignature()
        unsigned.clearKey()
        guard let bytes = try? unsigned.serializedData() else { return false }
        return (try? signer.isValidSignature(message.signature, for: Self.signaturePrefix + bytes)) == true
    }

    /// Whether the PeerID embeds its public key (an identity multihash, whose code is `0x00`)
    static func hasInlinedPublicKey(_ peerID: PeerID) -> Bool {
        peerID.id.first == 0x00
    }
}
