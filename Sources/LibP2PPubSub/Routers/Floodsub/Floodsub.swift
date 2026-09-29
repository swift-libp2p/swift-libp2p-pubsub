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

/// The FloodSub router (`/floodsub/1.0.0`), every message is flooded to every peer subscribed to its topic.
///
/// [Spec](https://github.com/libp2p/specs/blob/master/pubsub/README.md)
///
/// Register it with `app.pubsub.use(.floodsub)` and access it via `app.pubsub.floodsub`.
public final class FloodSub: PubSubService, PubSubCore, LifecycleHandler, @unchecked Sendable {
    public static let multicodec: String = "/floodsub/1.0.0"

    public init(application: Application, configuration: PubSubConfiguration = .init()) {
        super.init(
            application: application,
            protocolIDs: [FloodSub.multicodec],
            name: "Floodsub",
            configuration: configuration,
            router: FloodSubRouter(),
            registerRoute: { app, handlers, handler in
                app.group("floodsub") { $0.on("1.0.0", handlers: handlers, use: handler) }
            }
        )
    }
}
