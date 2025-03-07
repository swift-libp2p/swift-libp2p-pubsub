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

import XCTest
@testable import LibP2PPubSub
import LibP2P
import LibP2PNoise
import LibP2PMPLEX

final class LibP2PPubSubTests: XCTestCase {
    
    func testExample() throws {
        let app = try Application(.testing, peerID: PeerID(.Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.security.use(.noise)
        app.muxers.use(.mplex)
        app.pubsub.use(.floodsub)
        
        try app.start()
        
        sleep(2)
        
        app.shutdown()
    }
    
}
