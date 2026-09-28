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

/// A subscription to a topic, delivering events as an `AsyncSequence`.
///
/// ```swift
/// let subscription = try await app.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
/// for await message in subscription.messages {
///     print(String(decoding: message.data, as: UTF8.self))
/// }
/// ```
///
/// The subscription remains active until it is cancelled (via ``cancel()``, by cancelling the task
/// who's iterating it, or by letting it deinitialize), or until the router unsubscribes from the topic.
///
/// - Note: Each subscription buffers up to ``PubSubConfiguration/subscriptionBufferSize`` events.
///   Events arriving while the buffer is full are dropped, so keep up with the sequence.
///
/// - Important: Like any `AsyncStream`, a subscription only supports a single consumer.
public struct PubSubSubscription: AsyncSequence, Sendable {
    public typealias Element = PubSub.SubscriptionEvent

    /// The topic this subscription is for
    public let topic: String

    private let events: AsyncStream<PubSub.SubscriptionEvent>
    private let onCancel: @Sendable () -> Void

    init(topic: String, events: AsyncStream<PubSub.SubscriptionEvent>, onCancel: @escaping @Sendable () -> Void) {
        self.topic = topic
        self.events = events
        self.onCancel = onCancel
    }

    public func makeAsyncIterator() -> AsyncStream<PubSub.SubscriptionEvent>.Iterator {
        self.events.makeAsyncIterator()
    }

    /// Just the messages published to the topic
    public var messages: AsyncCompactMapSequence<PubSubSubscription, PubSubMessage> {
        self.compactMap { event in
            guard case .data(let message) = event else { return nil }
            return message
        }
    }

    /// Ends the subscription.
    ///
    /// - Note: If this is the last subscription to the topic, it will trigger an unsubscribe from the topic.
    public func cancel() {
        self.onCancel()
    }
}
