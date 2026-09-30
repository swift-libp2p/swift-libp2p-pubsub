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

/// Describes how we participate in a topic, which messages we accept and how we identify them.
public struct TopicConfiguration: Sendable {

    /// The topic string
    public var topic: String

    /// The signature policy messages on this topic must conform to.
    ///
    /// - Important: Every peer on a topic must use the same policy.
    public var signaturePolicy: PubSub.SignaturePolicy

    /// The MessageValidator used to determine if a message on this topic is valid and should be accepted / forwarded.
    public var validator: MessageValidator

    /// How messages on this topic are identified (used for duplicate suppression and gossip).
    ///
    /// - Important: Every peer on a topic must use the same strategy, otherwise IHAVE / IWANT gossip can't work.
    public var messageID: MessageIDStrategy

    public init(
        topic: String,
        signaturePolicy: PubSub.SignaturePolicy = .strictSign,
        validator: MessageValidator = .acceptAll,
        messageID: MessageIDStrategy = .fromAndSequenceNumber
    ) {
        self.topic = topic
        self.signaturePolicy = signaturePolicy
        self.validator = validator
        self.messageID = messageID
    }

    /// Bridges a swift-libp2p-core `PubSub.SubscriptionConfig`.
    /// - Note: We bypass cores hashing due to it using swifts built in hasher instead of SHA256
    ///   which is needed for stable / deterministic hashing.
    /// - Todo: Update swift-libp2p-core to use swift-crypto's SHA256 hasher.
    public init(_ config: PubSub.SubscriptionConfig) {
        let validator: MessageValidator
        switch config.validator {
        case .acceptAll:
            validator = .acceptAll
        case .custom(let isValid):
            validator = .predicate(isValid)
        }

        let messageID: MessageIDStrategy
        switch config.messageIDFunc {
        case .concatFromAndSequenceFields:
            messageID = .fromAndSequenceNumber
        case .hashSequenceNumberAndFromFields:
            messageID = .hashedSequenceNumberAndFrom
        case .hashEverything:
            messageID = .hashedMessage
        case .custom(let function):
            messageID = .custom(function)
        }

        self.init(
            topic: config.topic,
            signaturePolicy: config.signaturePolicy,
            validator: validator,
            messageID: messageID
        )
    }

    /// The message ID strategy we actually use for this topic.
    ///
    /// Strategies that depend on the `from` and `seqno` fields would give every message the same ID under
    /// StrictNoSign (because those fields are required to be empty), so a content based ID is used instead.
    var effectiveMessageID: MessageIDStrategy {
        guard case .strictNoSign = signaturePolicy, messageID.dependsOnAuthorship else { return messageID }
        return .contentHash
    }
}
