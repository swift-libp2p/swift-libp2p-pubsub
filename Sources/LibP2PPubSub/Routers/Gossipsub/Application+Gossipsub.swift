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

    /// GossipSub with the default configuration and parameters
    public static var gossipsub: Self {
        .gossipsub(configuration: .init())
    }

    /// GossipSub
    /// - Parameters:
    ///   - configuration: The core PubSub Configuration params
    ///   - parameters: GossipSub configuration params (mesh size, history legth, etc)
    public static func gossipsub(
        configuration: PubSubConfiguration,
        parameters: GossipSubParameters = .init()
    ) -> Self {
        .init {
            $0.pubsub.use { app -> GossipSub in
                let gsub = GossipSub(application: app, configuration: configuration, parameters: parameters)
                app.lifecycle.use(gsub)
                return gsub
            }
        }
    }

    @available(*, deprecated, renamed: "gossipsub(configuration:parameters:)")
    public static func gossipsub(emitSelf: Bool) -> Self {
        .gossipsub(configuration: .init(emitSelf: emitSelf))
    }
}

extension Application.PubSubServices {
    public var gossipsub: GossipSub {
        guard let gsub = self.service(for: GossipSub.self) else {
            fatalError(
                "Gossipsub accessed without instantiating it first. Use app.pubsub.use(.gossipsub) to initialize a shared Gossipsub instance."
            )
        }
        return gsub
    }
}
