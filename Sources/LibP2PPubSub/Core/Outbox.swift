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

/// Consolidates the outbound RPC messages a router wants to send into a single message per peer.
///
/// This gives us control message piggybacking, a GRAFT, an IHAVE and a forwarded message
/// generated while handling one event will all leave in the same frame.
struct Outbox {
    private(set) var rpcs: [PeerID: RPC] = [:]

    /// Peers the router would like us to connect to (ex: direct peers we've lost, or peers learnt via PX)
    private(set) var dials: Set<PeerID> = []

    /// The (verified) peer records we received for the peers we'd like to dial, which tell us where to find them
    private(set) var dialRecords: [PeerID: PeerRecord] = [:]

    var isEmpty: Bool { self.rpcs.isEmpty && self.dials.isEmpty }

    mutating func dial(_ peer: PeerID, record: PeerRecord? = nil) {
        self.dials.insert(peer)
        if let record { self.dialRecords[peer] = record }
    }

    mutating func send(_ rpc: RPC, to peer: PeerID) {
        if var existing = self.rpcs[peer] {
            existing.merge(rpc)
            self.rpcs[peer] = existing
        } else {
            self.rpcs[peer] = rpc
        }
    }

    mutating func send(messages: [RPC.Message], to peer: PeerID) {
        guard !messages.isEmpty else { return }
        self.send(RPC.with { $0.msgs = messages }, to: peer)
    }

    mutating func send(control: RPC.ControlMessage, to peer: PeerID) {
        self.send(RPC.with { $0.control = control }, to: peer)
    }

    mutating func graft(_ topic: String, to peer: PeerID) {
        let graft = RPC.ControlGraft.with { $0.topicID = topic }
        self.send(
            control: .with { ctrlMsg in
                ctrlMsg.graft = [graft]
            },
            to: peer
        )
    }

    /// Record a Prune control message for `peer` on `topic`
    /// - Parameters:
    ///   - backoff: How long the peer should wait before grafting us again (sent in whole seconds)
    ///   - peers: The peers we suggest (PX, if peer exchange is enabled)
    ///   - signedRecords: The signed peer record envelopes we have for (some of) the suggested peers
    mutating func prune(
        _ topic: String,
        to peer: PeerID,
        backoff: Duration? = nil,
        peers: [PeerID] = [],
        signedRecords: [PeerID: Data] = [:]
    ) {
        let prune = RPC.ControlPrune.with { prune in
            prune.topicID = topic
            if let backoff { prune.backoff = UInt64(max(0, backoff.components.seconds)) }
            prune.peers = peers.map { exchanged in
                .with { info in
                    info.peerID = Data(exchanged.id)
                    if let record = signedRecords[exchanged] { info.signedPeerRecord = record }
                }
            }
        }
        self.send(
            control: .with { ctrlMsg in
                ctrlMsg.prune = [prune]
            },
            to: peer
        )
    }

    /// GossipSub v1.2, tells the peer not to send us the specified messages
    mutating func dontWant(_ ids: [Data], to peer: PeerID) {
        guard !ids.isEmpty else { return }
        self.send(control: .with { $0.idontwant = [.with { $0.messageIds = ids }] }, to: peer)
    }

    mutating func merge(_ other: Outbox) {
        for (peer, rpc) in other.rpcs { self.send(rpc, to: peer) }
        self.dials.formUnion(other.dials)
        self.dialRecords.merge(other.dialRecords) { _, new in new }
    }
}

extension RPC {
    /// Appends the contents of `other` to this RPC
    mutating func merge(_ other: RPC) {
        self.subscriptions.append(contentsOf: other.subscriptions)
        self.msgs.append(contentsOf: other.msgs)
        if other.hasControl {
            var control = self.control
            control.ihave.append(contentsOf: other.control.ihave)
            control.iwant.append(contentsOf: other.control.iwant)
            control.graft.append(contentsOf: other.control.graft)
            control.prune.append(contentsOf: other.control.prune)
            control.idontwant.append(contentsOf: other.control.idontwant)
            self.control = control
        }
    }

    /// Splits this RPC into RPCs whose serialized size doesn't exceed `maxSize`, where possible.
    ///
    /// Subscriptions and control messages travel together, each published message travels on its own.
    /// A single message that is larger than `maxSize` on its own can't be split and is dropped.
    func fragmented(maxSize: Int) -> (fragments: [RPC], droppedMessages: Int) {
        if let size = try? self.serializedData().count, size <= maxSize { return ([self], 0) }

        var fragments: [RPC] = []
        var dropped = 0
        if !self.subscriptions.isEmpty || self.hasControl {
            var header = RPC()
            header.subscriptions = self.subscriptions
            if self.hasControl { header.control = self.control }
            fragments.append(header)
        }
        for message in self.msgs {
            let fragment = RPC.with { $0.msgs = [message] }
            if let size = try? fragment.serializedData().count, size <= maxSize {
                fragments.append(fragment)
            } else {
                dropped += 1
            }
        }
        return (fragments, dropped)
    }
}
