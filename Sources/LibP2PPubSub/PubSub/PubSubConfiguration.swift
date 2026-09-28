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

/// Settings shared by every PubSub router.
///
/// The defaults match go-libp2p-pubsub.
public struct PubSubConfiguration: Sendable {
    /// How often the router performs its periodic maintenance (mesh upkeep, gossip emission, cache expiry).
    public var heartbeatInterval: Duration

    /// How long a message ID is remembered (for duplicate suppression).
    public var seenTTL: Duration

    /// The largest RPC message we accept or send (in bytes).
    public var maxMessageSize: Int

    /// The number of RPC messages that we'll queue for a single peer before new ones are dropped.
    public var outboundQueueSize: Int

    /// The number of events buffered for each ``PubSubSubscription`` before new events are dropped.
    public var subscriptionBufferSize: Int

    /// The signature policy used when publishing to a topic we're not subscribed to.
    public var defaultSignaturePolicy: PubSub.SignaturePolicy

    /// The longest a topic's validators may take to decide on a message before it's ignored. `nil` waits indefinitely.
    public var validationTimeout: Duration?

    /// Whether the messages we publish are also delivered to our own subscriptions.
    public var emitSelf: Bool

    public init(
        heartbeatInterval: Duration = .seconds(1),
        seenTTL: Duration = .seconds(120),
        maxMessageSize: Int = 1 << 20,
        outboundQueueSize: Int = 32,
        subscriptionBufferSize: Int = 32,
        defaultSignaturePolicy: PubSub.SignaturePolicy = .strictSign,
        validationTimeout: Duration? = nil,
        emitSelf: Bool = false
    ) {
        precondition(heartbeatInterval > .zero, "The heartbeat interval must be greater than zero")
        precondition(maxMessageSize > 0, "The max message size must be greater than zero")
        precondition(outboundQueueSize > 0, "The outbound queue size must be greater than zero")
        precondition(subscriptionBufferSize > 0, "The subscription buffer size must be greater than zero")
        self.heartbeatInterval = heartbeatInterval
        self.seenTTL = seenTTL
        self.maxMessageSize = maxMessageSize
        self.outboundQueueSize = outboundQueueSize
        self.subscriptionBufferSize = subscriptionBufferSize
        self.defaultSignaturePolicy = defaultSignaturePolicy
        self.validationTimeout = validationTimeout
        self.emitSelf = emitSelf
    }
}
