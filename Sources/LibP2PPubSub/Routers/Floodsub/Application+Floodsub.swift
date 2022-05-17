//
//  Application+Floodsub.swift
//  
//
//  Created by Brandon Toms on 4/19/22.
//

import LibP2P

extension Application.PubSubServices.Provider {
    public static var floodsub: Self {
        .init {
            $0.pubsub.use { app -> FloodSub in
                let fsub = try! FloodSub(group: app.eventLoopGroup, libp2p: app)
                app.lifecycle.use(fsub)
                return fsub
            }
        }
    }
    
    public static func floodsub(emitSelf:Bool) -> Self {
        .init {
            $0.pubsub.use { app -> FloodSub in
                let fsub = try! FloodSub(group: app.eventLoopGroup, libp2p: app, debugName: "Floodsub", emitSelf: emitSelf)
                app.lifecycle.use(fsub)
                return fsub
            }
        }
    }
}

extension Application.PubSubServices {
    
    public var floodsub:FloodSub {
        guard let fsub = self.service(for: FloodSub.self) else {
            fatalError("Floodsub accessed without instantiating it first. Use app.pubsub.use(.floodsub) to initialize a shared Floodsub instance.")
        }
        return fsub
    }
}
