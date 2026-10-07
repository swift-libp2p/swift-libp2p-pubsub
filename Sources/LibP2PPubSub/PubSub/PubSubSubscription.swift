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

/// A subscription to a topic, delivering events as an `AsyncSequence` (swift-libp2p-core's `PubSub.Subscription`).
///
/// ```swift
/// let subscription = try await app.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "fruit"))
/// for await message in subscription.messages {
///     print(String(decoding: message.data, as: UTF8.self))
/// }
/// ```
///
/// The subscription remains active until it is cancelled (via `cancel()`, by cancelling the task
/// who's iterating it, or by letting it deinitialize), or until the router unsubscribes from the topic.
/// If it's the last subscription to the topic, cancelling it unsubscribes us from the topic.
///
/// - Note: Each subscription buffers up to ``PubSubConfiguration/subscriptionBufferSize`` events.
///   Events arriving while the buffer is full are dropped, so keep up with the sequence.
///
/// - Important: Like any `AsyncStream`, a subscription only supports a single consumer.
public typealias PubSubSubscription = PubSub.Subscription
