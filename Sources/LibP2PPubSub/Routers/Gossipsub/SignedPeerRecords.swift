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

/// A verified `PeerRecord`, along with the signed envelope it arrived in.
///
/// GossipSub v1.1 peer exchange carries each suggested peer's signed peer record, so the receiver can connect to a peer
/// it has never seen. We can't sign another peer's record, so we keep the original envelope in order to pass it on.
struct SignedPeerRecord: Sendable {
    enum VerificationError: Error {
        /// The record inside the envelope belongs to a different peer than the one that signed it
        case signerMismatch
    }

    /// The (verified) record
    let record: PeerRecord

    /// The marshaled envelope containing the record, exactly as the peer signed it
    let envelope: Data

    /// The peer the record describes (and that signed it)
    var peer: PeerID { self.record.peerID }

    /// Opens a marshaled envelope, verifying its signature and that the record belongs to the peer that signed it
    init(envelope: Data) throws {
        let sealed = try SealedEnvelope(marshaledEnvelope: envelope.byteArray)
        let record = try PeerRecord(marshaledData: Data(sealed.rawPayload))
        guard record.peerID == sealed.pubKey else { throw VerificationError.signerMismatch }
        self.record = record
        self.envelope = envelope
    }

    /// The signed peer record in a serialized identify message, if the message has one, the record verifies and belongs
    /// to `peer`, and the peer speaks at least one of our `protocols`.
    ///
    /// - Note: swift-libp2p's `IdentifyMessage` isn't public, so we read the two fields we need straight off the wire.
    init?(identify message: [UInt8], from peer: PeerID, speakingAnyOf protocols: [String]) {
        guard let fields = IdentifyFields(message),
            let envelope = fields.signedPeerRecord,
            !Set(fields.protocols).isDisjoint(with: protocols),
            let signed = try? SignedPeerRecord(envelope: envelope),
            signed.peer == peer
        else { return nil }
        self = signed
    }
}

/// The most recent signed peer record we've received from each peer, used to attach records to the peers we suggest via PX.
///
/// The book holds at most `capacity` records. When it's full, records of peers we're no longer connected to make way for new ones.
///
/// - Note: Our (swift-libp2p) peer store only keeps the unsigned `PeerRecord`, not the envelope it arrived in, so we collect
///   envelopes ourselves. Once the peer store keeps signed envelopes, this book (and the identify parsing below) can go.
struct SignedPeerRecordBook {
    let capacity: Int
    private(set) var records: [PeerID: SignedPeerRecord] = [:]

    init(capacity: Int = 1024) {
        self.capacity = capacity
    }

    subscript(peer: PeerID) -> SignedPeerRecord? {
        self.records[peer]
    }

    /// Stores the record, unless we already hold a more recent one for the peer, or the book is full of connected peers' records
    ///
    /// - Parameter connected: The peers we're currently connected to, whose records are kept when making room
    mutating func insert(_ record: SignedPeerRecord, connected: Set<PeerID>) {
        if let existing = self.records[record.peer] {
            guard record.record.sequenceNumber > existing.record.sequenceNumber else { return }
        } else if self.records.count >= self.capacity {
            self.records = self.records.filter { connected.contains($0.key) }
            guard self.records.count < self.capacity else { return }
        }
        self.records[record.peer] = record
    }
}

/// The fields of an identify message (`/ipfs/id/1.0.0`) we need, read directly from the protobuf wire format
private struct IdentifyFields {
    /// `repeated string protocols = 3`
    var protocols: [String] = []
    /// `optional bytes signedPeerRecord = 8`
    var signedPeerRecord: Data?

    init?(_ bytes: [UInt8]) {
        var reader = ProtobufReader(bytes)
        while !reader.isAtEnd {
            guard let key = reader.varint() else { return nil }
            let (field, wireType) = (key >> 3, key & 0x7)
            switch (field, wireType) {
            case (3, 2):
                guard let value = reader.lengthDelimited() else { return nil }
                self.protocols.append(String(decoding: value, as: UTF8.self))
            case (8, 2):
                guard let value = reader.lengthDelimited() else { return nil }
                self.signedPeerRecord = Data(value)
            default:
                guard reader.skip(wireType: wireType) else { return nil }
            }
        }
    }
}

/// A minimal protobuf wire format reader
private struct ProtobufReader {
    private let bytes: [UInt8]
    private var index = 0

    init(_ bytes: [UInt8]) {
        self.bytes = bytes
    }

    var isAtEnd: Bool { self.index >= self.bytes.count }

    mutating func varint() -> UInt64? {
        var value: UInt64 = 0
        var shift: UInt64 = 0
        while self.index < self.bytes.count, shift < 64 {
            let byte = self.bytes[self.index]
            self.index += 1
            value |= UInt64(byte & 0x7F) << shift
            if byte & 0x80 == 0 { return value }
            shift += 7
        }
        return nil
    }

    mutating func lengthDelimited() -> ArraySlice<UInt8>? {
        guard let length = self.varint(), length <= UInt64(self.bytes.count - self.index) else { return nil }
        let end = self.index + Int(length)
        defer { self.index = end }
        return self.bytes[self.index..<end]
    }

    /// Skips over a field's value, returning `false` if the value is malformed (or uses a deprecated group wire type)
    mutating func skip(wireType: UInt64) -> Bool {
        switch wireType {
        case 0: return self.varint() != nil
        case 1: return self.advance(8)
        case 2: return self.lengthDelimited() != nil
        case 5: return self.advance(4)
        default: return false
        }
    }

    private mutating func advance(_ count: Int) -> Bool {
        guard self.bytes.count - self.index >= count else { return false }
        self.index += count
        return true
    }
}
