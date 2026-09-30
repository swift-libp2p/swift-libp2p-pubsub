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

extension Application.PubSubServices.Provider {
    /// FloodSub with the default configuration
    public static var floodsub: Self {
        .floodsub(configuration: .init())
    }

    public static func floodsub(configuration: PubSubConfiguration) -> Self {
        .init {
            $0.pubsub.use { app -> FloodSub in
                let fsub = FloodSub(application: app, configuration: configuration)
                app.lifecycle.use(fsub)
                return fsub
            }
        }
    }

    @available(*, deprecated, renamed: "floodsub(configuration:)")
    public static func floodsub(emitSelf: Bool) -> Self {
        .floodsub(configuration: .init(emitSelf: emitSelf))
    }
}

extension Application.PubSubServices {
    public var floodsub: FloodSub {
        guard let fsub = self.service(for: FloodSub.self) else {
            fatalError(
                "Floodsub accessed without instantiating it first. Use app.pubsub.use(.floodsub) to initialize a shared Floodsub instance."
            )
        }
        return fsub
    }
}
