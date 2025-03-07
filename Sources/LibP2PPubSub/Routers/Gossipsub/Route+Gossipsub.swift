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

func registerGossipsubRoute(_ app:Application) throws {
    app.group("meshsub") { msub in

        msub.on("1.0.0", handlers: [.varIntFrameDecoder]) { req -> EventLoopFuture<Response<ByteBuffer>> in
            
            guard req.application.isRunning else {
                req.logger.error("Gossipsub::Recieved Request After App Shutdown")
                return req.eventLoop.makeFailedFuture(BasePubSub.Errors.alreadyStopped)
            }
            return req.application.pubsub.gossipsub.processRequest(req)
            
        }
    }
}
