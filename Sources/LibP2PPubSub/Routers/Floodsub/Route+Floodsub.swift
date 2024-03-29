//
//  Route+Floodsub.swift
//  
//
//  Created by Brandon Toms on 4/18/22.
//

import LibP2P

func registerFloodsubRoute(_ app:Application) throws {
    app.group("floodsub") { fsub in

        fsub.on("1.0.0", handlers: [.varIntFrameDecoder]) { req -> EventLoopFuture<Response<ByteBuffer>> in
            
            guard req.application.isRunning else {
                req.logger.error("Floodsub::Recieved Request After App Shutdown")
                return req.eventLoop.makeFailedFuture(BasePubSub.Errors.alreadyStopped)
            }
            return req.application.pubsub.floodsub.processRequest(req)
            
        }
    }
}
