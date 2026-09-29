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

/// GossipSub's sliding window message cache (`mcache`).
///
/// Messages are retrievable (for IWANT requests) for `historyLength` shifts, but only advertised
/// (via IHAVE gossip) for the most recent `gossipLength` shifts. The slack between the two accounts for the
/// time between a peer receiving our IHAVE and its IWANT reaching us.
///
/// The router shifts the cache once per heartbeat.
struct MessageCache {
    struct Entry {
        let topic: String
        let message: RPC.Message
    }

    /// `mcache_len`
    let historyLength: Int

    /// `mcache_gossip`
    let gossipLength: Int

    /// A dictionary holding the actual RPC.Message, keyed by it's ID
    private var entries: [Data: Entry] = [:]

    /// `windows[0]` holds the IDs of the most recent messages
    private var windows: [[Data]]

    init(historyLength: Int, gossipLength: Int) {
        precondition(
            0 < gossipLength && gossipLength <= historyLength,
            "Invalid message cache parameters: 0 < gossipLength [\(gossipLength)] <= historyLength [\(historyLength)]"
        )
        self.historyLength = historyLength
        self.gossipLength = gossipLength
        self.windows = [[]]
    }

    var count: Int { self.entries.count }

    func contains(_ id: Data) -> Bool {
        self.entries[id] != nil
    }

    /// Stores a message in the current window. Returns `false` if it's already cached.
    @discardableResult
    mutating func put(_ id: Data, message: RPC.Message, topic: String) -> Bool {
        guard self.entries[id] == nil else { return false }
        self.entries[id] = Entry(topic: topic, message: message)
        self.windows[0].append(id)
        return true
    }

    /// Get the RPC.Message by it's ID
    func get(_ id: Data) -> RPC.Message? {
        self.entries[id]?.message
    }

    /// The IDs of the messages on `topic` within the gossip window, newest first
    func gossipIDs(for topic: String) -> [Data] {
        self.windows.prefix(self.gossipLength).flatMap { window in
            window.filter { self.entries[$0]?.topic == topic }
        }
    }

    /// Starts a new window, evicting the messages in windows older than `historyLength`
    mutating func shift() {
        self.windows.insert([], at: 0)
        while self.windows.count > self.historyLength {
            for id in self.windows.removeLast() {
                self.entries.removeValue(forKey: id)
            }
        }
    }
}
