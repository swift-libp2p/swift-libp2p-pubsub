// swift-tools-version: 6.1
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

import PackageDescription

let package = Package(
    name: "swift-libp2p-pubsub",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "LibP2PPubSub",
            targets: ["LibP2PPubSub"]
        )
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/swift-libp2p/swift-libp2p.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/apple/swift-crypto.git", "3.0.0"..<"5.0.0"),

        // Test dependencies
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-noise.git", .upToNextMinor(from: "0.4.0")),
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-yamux.git", .upToNextMinor(from: "0.4.0")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "LibP2PPubSub",
            dependencies: [
                .product(name: "LibP2P", package: "swift-libp2p"),
                .product(name: "Crypto", package: "swift-crypto"),
            ],
            resources: [
                .copy("Protobufs/RPC.proto"),
                .copy("Protobufs/RPC2.proto"),
            ]
        ),
        .testTarget(
            name: "LibP2PPubSubTests",
            dependencies: [
                "LibP2PPubSub",
                .product(name: "LibP2PNoise", package: "swift-libp2p-noise"),
                .product(name: "LibP2PYAMUX", package: "swift-libp2p-yamux"),
            ]
        ),
    ]
)
