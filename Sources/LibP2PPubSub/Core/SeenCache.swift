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

/// Keeps track of recently seen message IDs, for duplicate suppression.
///
/// Entries expire `ttl` after they were first seen (seeing a message again doesn't extend its lifetime),
/// matching go-libp2p-pubsub's default `FirstSeen` time cache.
struct SeenCache {
    typealias Instant = ContinuousClock.Instant

    let ttl: Duration

    /// Message ID -> expiry
    private var expiries: [Data: Instant] = [:]

    /// Insertion ordered (and therefore expiry ordered) IDs, consumed from `head`
    private var order: [(id: Data, expiry: Instant)] = []

    /// The index into our order array
    private var head: Int = 0

    init(ttl: Duration) {
        self.ttl = ttl
    }

    var count: Int { self.expiries.count }

    func contains(_ id: Data) -> Bool {
        self.expiries[id] != nil
    }

    /// Records `id` as seen. Returns `false` if it had already been seen.
    @discardableResult
    mutating func insert(_ id: Data, now: Instant) -> Bool {
        guard self.expiries[id] == nil else { return false }
        let expiry = now + self.ttl
        self.expiries[id] = expiry
        self.order.append((id, expiry))
        return true
    }

    /// Forgets the IDs whose TTL has elapsed
    mutating func prune(now: Instant) {
        while self.head < self.order.count, self.order[self.head].expiry <= now {
            self.expiries.removeValue(forKey: self.order[self.head].id)
            self.head += 1
        }
        /// Compact the backing storage once the consumed prefix dominates it
        if self.head > 1024 && self.head * 2 > self.order.count {
            self.order.removeFirst(self.head)
            self.head = 0
        }
    }
}
