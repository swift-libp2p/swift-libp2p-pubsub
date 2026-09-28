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

/// Validates an inbound message, ensuring it complies with the topic/subscription specific rules.
///
/// Validators only see messages that conform to the topic's signature policy and that we haven't seen before.
/// They run concurrently with the rest of the router, so they may be slow (ex: consult a database), although
/// ``PubSubConfiguration/validationTimeout`` can bound how long we wait.
public struct MessageValidator: Sendable {
    private let body: @Sendable (_ message: PubSubMessage, _ messenger: PeerID) async -> PubSub.ValidationResult

    /// - Parameter body: Returns the `message`s validity, which was forwarded to us by `messenger` (not necessarily its author).
    ///   - `.accept`: deliver the message to our subscribers and forward it to the network
    ///   - `.reject`: drop the message, it's invalid (routers with peer scoring penalize the messenger)
    ///   - `.ignore` / `.throttle`: drop the message without penalizing anyone
    public init(
        _ body:
            @escaping @Sendable (_ message: PubSubMessage, _ messenger: PeerID) async -> PubSub.ValidationResult
    ) {
        self.body = body
    }

    /// Accepts every message
    public static var acceptAll: MessageValidator {
        MessageValidator { _, _ in .accept }
    }

    /// Accepts messages for which `isValid` returns `true` and rejects the rest
    public static func predicate(_ isValid: @escaping @Sendable (PubSubMessage) -> Bool) -> MessageValidator {
        MessageValidator { message, _ in isValid(message) ? .accept : .reject }
    }

    func validate(_ message: PubSubMessage, from messenger: PeerID) async -> PubSub.ValidationResult {
        await self.body(message, messenger)
    }
}
