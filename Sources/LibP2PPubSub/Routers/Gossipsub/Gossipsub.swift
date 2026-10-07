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

/// The GossipSub router (`/meshsub/1.2.0`, `/meshsub/1.1.0` and `/meshsub/1.0.0`).
///
/// Messages are forwarded to a bounded mesh of peers per topic, while the remaining topic peers are told about recent
/// messages via gossip and can request any they may have missed. We speak the latest version each peer supports.
///
/// By default we also speak `/floodsub/1.0.0` (see ``GossipSubParameters/floodSubCompatible``)
/// - FloodSub-only peers can take part in our topics
/// - But they're never grafted into a mesh or sent gossip messages
///
/// Specs:
/// - [v1.0](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md)
/// - [v1.1](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md) (peer scoring is opt-in, see ``GossipSubParameters/scoring``)
/// - [v1.2](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md)
///
/// Register it with `app.pubsub.use(.gossipsub)` and access it via `app.pubsub.gossipsub`.
public final class GossipSub: PubSubService, PubSubCore, AsyncPubSub, LifecycleHandler, @unchecked Sendable {
    /// Our preferred (newest) protocol
    public static let multicodec: String = GossipSub.v1_2

    static let v1_2 = "/meshsub/1.2.0"
    static let v1_1 = "/meshsub/1.1.0"
    static let v1_0 = "/meshsub/1.0.0"

    /// The mesh and gossip parameters this router was configured with
    public let parameters: GossipSubParameters

    public init(
        application: Application,
        configuration: PubSubConfiguration = .init(),
        parameters: GossipSubParameters = .init()
    ) {
        self.parameters = parameters
        let floodSubCompatible = parameters.floodSubCompatible
        let gossipSubProtocols = [GossipSub.v1_2, GossipSub.v1_1, GossipSub.v1_0]

        /// We know the addresses of our direct peers, so we can (re)connect to them ourselves
        var directAddresses: [PeerID: Multiaddr] = [:]
        for address in parameters.directPeers {
            guard let peer = address.getPeerIDString().flatMap({ try? PeerID(cid: $0) }) else {
                application.logger.warning("Ignoring GossipSub direct peer `\(address)` without a `/p2p/` component")
                continue
            }
            directAddresses[peer] = address
        }

        super.init(
            application: application,
            protocolIDs: floodSubCompatible ? gossipSubProtocols + [FloodSub.multicodec] : gossipSubProtocols,
            name: "Gossipsub",
            configuration: configuration,
            router: GossipSubRouter(parameters: parameters),
            knownAddresses: directAddresses,
            registerRoute: { app, handlers, handler in
                app.group("meshsub") { meshsub in
                    meshsub.on("1.2.0", handlers: handlers, use: handler)
                    meshsub.on("1.1.0", handlers: handlers, use: handler)
                    meshsub.on("1.0.0", handlers: handlers, use: handler)
                }
                /// FloodSub-only peers can reach us over `/floodsub/1.0.0`, the engine handles multiple protocols
                if floodSubCompatible {
                    app.group("floodsub") { floodsub in
                        floodsub.on("1.0.0", handlers: handlers, use: handler)
                    }
                }
            }
        )
    }
}
