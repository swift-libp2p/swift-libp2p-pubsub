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

/// Restricts the topics we support.
///
/// Like go-libp2p-pubsub's `SubscriptionFilter`, this bounds the state a peer can make us keep, ex: by announcing
/// thousands of topics.
/// - Topics the filter doesn't allow
///   - can't be subscribed to locally (``PubSubError/topicNotAllowed(topic:)``)
///   - are ignored when a peer announces them.
/// - An RPC announcing more than ``maxSubscriptionsPerRPC`` subscriptions is ignored entirely.
public struct SubscriptionFilter: Sendable {
    private let isAllowed: @Sendable (_ topic: String) -> Bool

    /// The most subscription announcements a single RPC may contain. `nil` means unlimited.
    public var maxSubscriptionsPerRPC: Int?

    /// - Parameters:
    ///   - maxSubscriptionsPerRPC: The most subscription announcements a single RPC may contain. `nil` is unlimited.
    ///   - isAllowed: Returns `true` for the topics we're willing to subscribe to and track.
    public init(
        maxSubscriptionsPerRPC: Int? = nil,
        allowing isAllowed: @escaping @Sendable (_ topic: String) -> Bool
    ) {
        precondition((maxSubscriptionsPerRPC ?? 0) >= 0, "The max subscriptions per RPC can't be negative")
        self.maxSubscriptionsPerRPC = maxSubscriptionsPerRPC
        self.isAllowed = isAllowed
    }

    /// Allows every topic (the default)
    public static var allowAll: SubscriptionFilter {
        SubscriptionFilter { _ in true }
    }

    /// Only allows the specified topics
    public static func allowlist(_ topics: Set<String>, maxSubscriptionsPerRPC: Int? = nil) -> SubscriptionFilter {
        SubscriptionFilter(maxSubscriptionsPerRPC: maxSubscriptionsPerRPC) { topics.contains($0) }
    }

    /// Whether we're willing to subscribe to, and track peers subscribed to, `topic`
    public func allows(_ topic: String) -> Bool {
        self.isAllowed(topic)
    }
}
