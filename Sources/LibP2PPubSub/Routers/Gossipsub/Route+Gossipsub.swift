//
//  Route+Gossipsub.swift
//  
//
//  Created by Brandon Toms on 4/18/22.
//

import LibP2P

func registerGossipsubRoute(_ app:Application) throws {
    app.group("meshsub") { fsub in

        fsub.on("1.0.0", handlers: [.varIntFrameDecoder]) { req -> EventLoopFuture<ResponseType<ByteBuffer>> in
            
            guard req.application.isRunning else { return req.eventLoop.makeFailedFuture(BasePubSub.Errors.alreadyStopped) }
            return req.application.pubsub.gossipsub.processRequest(req)
            
        }
    }
}
