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
import Testing

@testable import LibP2PPubSub

@Suite("Libp2p PubSub Tests", .serialized)
struct LibP2PPubSubTests {

    @Test func testAppConfiguration_Floodsub() async throws {
        let app = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.pubsub.use(.floodsub)

        #expect(app.pubsub.available.map({ $0.description }) == ["/floodsub/1.0.0"])
        #expect(app.pubsub.service(for: FloodSub.self) != nil)
        #expect(app.pubsub.service(forKey: FloodSub.multicodec) != nil)

        try await app.startup()

        try await Task.sleep(for: .milliseconds(10))

        try await app.asyncShutdown()
    }

    @Test func testAppConfiguration_Gossipsub() async throws {
        let app = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.pubsub.use(.gossipsub)

        #expect(app.pubsub.available.map({ $0.description }) == ["/meshsub/1.2.0"])
        #expect(app.pubsub.service(for: GossipSub.self) != nil)
        #expect(app.pubsub.service(forKey: GossipSub.multicodec) != nil)

        try await app.startup()

        try await Task.sleep(for: .milliseconds(10))

        try await app.asyncShutdown()
    }

}

/// Thrown when an `AsyncSemaphore` isn't signaled within the allotted time
struct SemaphoreTimeoutError: Error, CustomStringConvertible {
    let timeout: Duration
    let sourceLocation: SourceLocation

    var description: String {
        "Timed out after \(timeout) waiting for semaphore at \(sourceLocation.fileName):\(sourceLocation.line)"
    }
}

extension AsyncSemaphore {
    /// Waits for the semaphore to be signaled, throwing a `SemaphoreTimeoutError` if it isn't signaled within `timeout`.
    ///
    /// - Note: `AsyncSemaphore.wait()` doesn't respect task cancellation, so a test's `.timeLimit` trait can't interrupt it.
    /// Use this method in tests so a missing signal fails fast (with the location of the offending wait) instead of stalling.
    func wait(timeout: Duration, sourceLocation: SourceLocation = #_sourceLocation) async throws {
        let signaled = try await withThrowingTaskGroup(of: Bool.self) { group in
            group.addTask {
                try await self.waitUnlessCancelled()
                return true
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                return false
            }
            /// Whichever child finishes first wins, the other gets cancelled
            let first = try await group.next() ?? false
            group.cancelAll()
            return first
        }
        guard signaled else { throw SemaphoreTimeoutError(timeout: timeout, sourceLocation: sourceLocation) }
    }
}

struct TestHelper {
    static var externalIntegrationTestsEnabled: Bool {
        if let b = ProcessInfo.processInfo.environment["PerformExternalIntegrationTests"], b == "true" {
            return true
        }
        return false
    }
}

extension Trait where Self == ConditionTrait {
    /// This test is only available when the `PerformExternalIntegrationTests` environment variable is set to `true`
    public static var externalIntegrationTestsEnabled: Self {
        enabled(
            if: TestHelper.externalIntegrationTestsEnabled,
            "This test is only available when the `PerformExternalIntegrationTests` environment variable is set to `true`"
        )
    }
}
