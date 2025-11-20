//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2025 swift-libp2p project authors
// Licensed under MIT
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of swift-libp2p project authors
//
// SPDX-License-Identifier: MIT
//
//===----------------------------------------------------------------------===//

import LibP2P
import LibP2PMPLEX
import LibP2PNoise
import Testing

@testable import LibP2PPubSub

@Suite("Libp2p PubSub Tests", .serialized)
struct LibP2PPubSubTests {

    @Test func testAppConfiguration_Floodsub() async throws {
        let app = try Application(.testing, peerID: PeerID(.Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.security.use(.noise)
        app.muxers.use(.mplex)
        app.pubsub.use(.floodsub)

        #expect(app.pubsub.available.map({ $0.description }) == ["/floodsub/1.0.0"])
        #expect(app.pubsub.service(for: FloodSub.self) != nil)
        #expect(app.pubsub.service(forKey: FloodSub.multicodec) != nil)

        try await app.startup()

        try await Task.sleep(for: .milliseconds(10))

        try await app.asyncShutdown()
    }

    @Test func testAppConfiguration_Gossipsub() async throws {
        let app = try Application(.testing, peerID: PeerID(.Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.security.use(.noise)
        app.muxers.use(.mplex)
        app.pubsub.use(.gossipsub)

        #expect(app.pubsub.available.map({ $0.description }) == ["/meshsub/1.0.0"])
        #expect(app.pubsub.service(for: GossipSub.self) != nil)
        #expect(app.pubsub.service(forKey: GossipSub.multicodec) != nil)

        try await app.startup()

        try await Task.sleep(for: .milliseconds(10))

        try await app.asyncShutdown()
    }

}

struct TestHelper {
    static var integrationTestsEnabled: Bool {
        if let b = ProcessInfo.processInfo.environment["PerformIntegrationTests"], b == "true" {
            return true
        }
        return false
    }
}

extension Trait where Self == ConditionTrait {
    /// This test is only available when the `PerformIntegrationTests` environment variable is set to `true`
    public static var externalIntegrationTestsEnabled: Self {
        enabled(
            if: TestHelper.integrationTestsEnabled,
            "This test is only available when the `PerformIntegrationTests` environment variable is set to `true`"
        )
    }
}
