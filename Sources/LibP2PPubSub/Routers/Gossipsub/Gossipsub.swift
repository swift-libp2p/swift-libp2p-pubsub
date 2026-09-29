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

/// The GossipSub router (`/meshsub/1.0.0`).
///
/// Messages are forwarded to a bounded mesh of peers per topic, while the remaining topic peers are told about recent
/// messages via gossip and can request any they may have missed.
///
/// By default we also speak `/floodsub/1.0.0` (see ``GossipSubParameters/floodSubCompatible``)
/// - FloodSub-only peers can take part in our topics
/// - But they're never grafted into a mesh or sent gossip messages
///
/// [Spec](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md)
///
/// Register it with `app.pubsub.use(.gossipsub)` and access it via `app.pubsub.gossipsub`.
public final class GossipSub: PubSubService, PubSubCore, LifecycleHandler, @unchecked Sendable {
    public static let multicodec: String = "/meshsub/1.0.0"

    /// The mesh and gossip parameters this router was configured with
    public let parameters: GossipSubParameters

    public init(
        application: Application,
        configuration: PubSubConfiguration = .init(),
        parameters: GossipSubParameters = .init()
    ) {
        self.parameters = parameters
        let floodSubCompatible = parameters.floodSubCompatible
        super.init(
            application: application,
            protocolIDs: floodSubCompatible ? [GossipSub.multicodec, FloodSub.multicodec] : [GossipSub.multicodec],
            name: "Gossipsub",
            configuration: configuration,
            router: GossipSubRouter(parameters: parameters),
            registerRoute: { app, handlers, handler in
                app.group("meshsub") { $0.on("1.0.0", handlers: handlers, use: handler) }
                /// FloodSub-only peers can reach us over `/floodsub/1.0.0`, the engine treats them accordingly
                if floodSubCompatible {
                    app.group("floodsub") { $0.on("1.0.0", handlers: handlers, use: handler) }
                }
            }
        )
    }
}
