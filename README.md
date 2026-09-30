# LibP2PPubSub

[![](https://img.shields.io/badge/made%20by-Breth-blue.svg?style=flat-square)](https://breth.app)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](http://libp2p.io/)
[![Swift Package Manager compatible](https://img.shields.io/badge/SPM-compatible-blue.svg?style=flat-square)](https://github.com/apple/swift-package-manager)
![Build & Test (macos and linux)](https://github.com/swift-libp2p/swift-libp2p-pubsub/actions/workflows/build+test.yml/badge.svg)

> FloodSub and GossipSub PubSub routers for swift-libp2p

> **Warning**
> This implementation hasn't been extensively tested yet. Please report any issues you encounter, here on github, so we can make this code better and safer for everyone!

## Table of Contents

- [Overview](#overview)
- [Install](#install)
- [Usage](#usage)
  - [Setup](#setup)
  - [Subscribing](#subscribing)
  - [Publishing](#publishing)
  - [Validation](#validation)
  - [Signature policies and message IDs](#signature-policies-and-message-ids)
  - [Configuration](#configuration)
  - [GossipSub parameters](#gossipsub-parameters)
  - [Peer scoring](#peer-scoring)
  - [Peer exchange and direct peers](#peer-exchange-and-direct-peers)
  - [Legacy (EventLoopFuture) API](#legacy-eventloopfuture-api)
- [Contributing](#contributing)
- [Credits](#credits)
- [License](#license)

## Overview
This repo contains the PubSub implementation for swift-libp2p. It provides two message routers:

- **FloodSub** (`/floodsub/1.0.0`) is the baseline protocol. It floods every message to every peer subscribed to its topic.
- **GossipSub** (`/meshsub/1.2.0`, `/meshsub/1.1.0` and `/meshsub/1.0.0`) builds a sparse mesh for each topic, and uses gossip to repair missed messages. GossipSub also speaks `/floodsub/1.0.0`, so FloodSub-only peers can join its topics.

| Spec | Support |
|---|---|
| [PubSub](https://github.com/libp2p/specs/blob/master/pubsub/README.md) | StrictSign and StrictNoSign signature policies, message validation (accept / reject / ignore), a seen cache, and a subscription filter |
| [GossipSub v1.0](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md) | Mesh maintenance, fanout, IHAVE / IWANT gossip, and control message piggybacking |
| [GossipSub v1.1](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md) | Prune backoff, peer exchange (with signed peer records), outbound mesh quotas, flood publishing, adaptive gossip, IHAVE / IWANT limits, direct peers, and opt-in peer scoring |
| [GossipSub v1.2](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.2.md) | IDONTWANT |

Each peer is spoken to using the newest protocol version it supports. Every default matches [go-libp2p-pubsub](https://github.com/libp2p/go-libp2p-pubsub).

The routers are built on Swift concurrency. An actor owns each router's state, validators are `async`, and subscriptions are `AsyncSequence`s. The `EventLoopFuture` API from swift-libp2p-core's `PubSubCore` still work using the legacy bridges.

## Install

Include the following dependency in your Package.swift file
```Swift
let package = Package(
    ...
    dependencies: [
        ...
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-pubsub.git", .upToNextMinor(from: "0.4.0"))
    ],
    ...
        .target(
            ...
            dependencies: [
                ...
                .product(name: "LibP2PPubSub", package: "swift-libp2p-pubsub"),
            ]),
    ...
)
```

## Usage
Check out the [tests](Tests/LibP2PPubSubTests) for more examples.

### Setup
This example uses Noise and Yamux, from [swift-libp2p-noise](https://github.com/swift-libp2p/swift-libp2p-noise) and [swift-libp2p-yamux](https://github.com/swift-libp2p/swift-libp2p-yamux). Any security and muxer modules will do.
```Swift
import LibP2P
import LibP2PNoise
import LibP2PPubSub
import LibP2PYAMUX

let app = try await Application.make(.production, peerID: .ephemeral(type: .Ed25519))
app.security.use(.noise)
app.muxers.use(.yamux)
app.servers.use(.tcp(host: "0.0.0.0", port: 0))

// Use GossipSub
app.pubsub.use(.gossipsub)
// Or FloodSub
app.pubsub.use(.floodsub)

try await app.startup()
```

You don't need to register peers with the router. Once we're connected to a peer and identify reports that it speaks one of the router's protocols, the router opens a stream to it and starts exchanging subscriptions. Most of the built-in bootstrap peers speak some pubsub protocol, dialing one of them should get you started.

### Subscribing
A subscription is an `AsyncSequence` of the topic's events.
```Swift
let subscription = try await app.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "news"))

// Every event
for await event in subscription {
    switch event {
    case .newPeer(let peer):
        print("\(peer) joined the topic")
    case .data(let message):
        print(String(decoding: message.data, as: UTF8.self))
    case .error(let error):
        print(error)
    }
}

// Or just the messages
for await message in subscription.messages {
    print(String(decoding: message.data, as: UTF8.self))
}
```

A subscription ends when you call `subscription.cancel()`, when the task iterating it is cancelled, or when the router stops. When the last subscription to a topic ends, we unsubscribe from the topic. To unsubscribe straight away, and end every subscription to the topic:
```Swift
await app.pubsub.gossipsub.unsubscribe(from: "news")
```

> **Note**
> Each subscription supports a single consumer, and buffers up to `PubSubConfiguration.subscriptionBufferSize` (32) events. Events that arrive while the buffer is full are dropped.

### Publishing
```Swift
try await app.pubsub.gossipsub.publish(Data("Hello".utf8), to: "news")
```
You don't need to subscribe to a topic to publish to it. Topics we haven't subscribed to use `PubSubConfiguration.defaultSignaturePolicy`.

You can also inspect the router:
```Swift
let topics = await app.pubsub.gossipsub.subscribedTopics()
let peers = await app.pubsub.gossipsub.peers(subscribedTo: "news")
```

### Validation
Validators are `async`. They only see messages that conform to the topic's signature policy and that we haven't seen before. They're given the message and the peer that forwarded it to us, which isn't necessarily the author.
```Swift
let validator = MessageValidator { message, messenger in
    guard message.data.count <= 1024 else { return .reject } // invalid: penalizes the messenger when scoring
    guard await isRelevant(message) else { return .ignore }  // not invalid, just not interesting
    return .accept                                            // deliver it, and forward it to the network
}
let subscription = try await app.pubsub.gossipsub.subscribe(TopicConfiguration(topic: "news", validator: validator))
```
`MessageValidator.predicate { message in ... }` is shorthand for a validator that accepts or rejects. Set `PubSubConfiguration.validationTimeout` to bound how long validation can take. A message still being validated when the timeout expires is ignored.

### Signature policies and message IDs
```Swift
TopicConfiguration(
    topic: "news",
    signaturePolicy: .strictSign,            // or .strictNoSign
    validator: .acceptAll,
    messageID: .fromAndSequenceNumber        // .hashedSequenceNumberAndFrom, .hashedMessage, .contentHash or .custom { message in ... }
)
```

> **Important**
> Every peer on a topic must use the same signature policy and the same message ID strategy. If peers derive message IDs differently, gossip can't work.

- **StrictSign** (the default): every message is signed, and carries its author (`from`) and a sequence number.
- **StrictNoSign**: messages carry none of these fields. Topics that use `from` and `seqno` for IDs fall back to `.contentHash` automatically, since otherwise every message would have the same ID.

### Configuration
`PubSubConfiguration` holds the settings shared by both routers:
```Swift
app.pubsub.use(.gossipsub(configuration: PubSubConfiguration(
    heartbeatInterval: .seconds(1),
    seenTTL: .seconds(120),
    maxMessageSize: 1 << 20,            // 1 MiB
    validationTimeout: .seconds(5),
    emitSelf: false,                    // deliver the messages we publish to our own subscriptions
    subscriptionFilter: .allowlist(["news", "weather"], maxSubscriptionsPerRPC: 100)
)))

app.pubsub.use(.floodsub(configuration: PubSubConfiguration(emitSelf: true)))
```
The subscription filter limits the topics we can subscribe to, and the topic subscriptions we track for our peers. `SubscriptionFilter(allowing:)` accepts a custom predicate.

### GossipSub parameters
`GossipSubParameters` holds GossipSub's mesh, gossip and v1.1 / v1.2 settings:
```Swift
app.pubsub.use(.gossipsub(
    configuration: PubSubConfiguration(),
    parameters: GossipSubParameters(
        meshDegree: 6,                  // D
        meshDegreeLow: 5,               // D_lo
        meshDegreeHigh: 12,             // D_hi
        gossipDegree: 6,                // D_lazy
        floodPublish: true,             // send our own messages to every topic peer, not just the mesh
        dontWantThreshold: 1024         // send IDONTWANT for messages of at least 1 KiB
    )
))
```
See `GossipSubParameters` for every option, including backoffs, gossip limits and IDONTWANT limits.

> **Note**
> GossipSub registers `/floodsub/1.0.0` as well, so that FloodSub-only peers can take part in its topics. If the same `Application` also runs the `FloodSub` router, set `GossipSubParameters(floodSubCompatible: false)`.

### Peer scoring
GossipSub v1.1 peer scoring is opt-in. Each peer's score is built from:
- how long it's been in our mesh;
- its first message deliveries and mesh delivery rate;
- invalid messages it sent;
- an application-specific score;
- how many peers share its IP address;
- its behaviour (for example, broken IWANT promises and GRAFTs during backoff).

Peers with low scores stop receiving gossip, then stop receiving our messages, and at the lowest scores their RPCs are ignored entirely. Scoring also affects which peers are kept in the mesh, and whose peer exchange suggestions we accept.
```Swift
let scoring = try GossipSubScoring(
    parameters: PeerScoreParameters(
        topics: ["news": TopicScoreParameters(topicWeight: 1)],
        appSpecificScore: { peer in trustedPeers.contains(peer) ? 10 : 0 },
        ipColocationFactorWeight: -10
    ),
    thresholds: PeerScoreThresholds(
        gossipThreshold: -10,
        publishThreshold: -50,
        graylistThreshold: -80,
        acceptPXThreshold: 10,
        opportunisticGraftThreshold: 1
    )
)
app.pubsub.use(.gossipsub(configuration: PubSubConfiguration(), parameters: GossipSubParameters(scoring: scoring)))
```
`GossipSubScoring` validates the parameters and throws `PeerScoreParameterError` if they're invalid. Only topics listed in `topics` contribute topic scores.

> **Important**
> Tune the score parameters for your application's traffic. For example, the mesh delivery penalty (`meshMessageDeliveriesWeight`) is disabled by default, because with go's defaults it penalises every peer on a quiet topic. See the spec's [parameter guidance](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#guidance-for-score-function-parameters).

### Peer exchange and direct peers
```Swift
GossipSubParameters(
    peerExchange: true,     // suggest other topic peers when we prune, and connect to the peers suggested to us
    directPeers: [try Multiaddr("/ip4/10.0.0.2/tcp/10000/p2p/12D3KooW...")]
)
```
- **Peer exchange** is off by default, as in go and rust. When it's on, our PRUNEs carry each suggested peer's signed peer record, so the receiver can reach a peer it has never seen. Records we receive are verified before we dial. Enable peer scoring as well if you can: then we only accept suggestions from peers that meet `acceptPXThreshold`.
- **Direct peers** always receive the messages on their topics, but are never part of a mesh. We reconnect to them if the connection drops. The addresses must include the peer's `/p2p/` ID, and the direct peer should also list us as a direct peer.

### Legacy (EventLoopFuture) API
Both routers still implement swift-libp2p-core's `PubSubCore`, so existing synchronous code keeps working. (In an `async` context, these calls resolve to core's `async` overloads.)
```Swift
let handler = try app.pubsub.gossipsub.subscribe(
    PubSub.SubscriptionConfig(topic: "news", signaturePolicy: .strictSign, validator: .acceptAll, messageIDFunc: .concatFromAndSequenceFields)
)
handler.on = { event -> EventLoopFuture<Void> in
    ...
    return app.eventLoopGroup.any().makeSucceededVoidFuture()
}

app.pubsub.gossipsub.publish(topic: "news", data: Data("Hello".utf8))
app.pubsub.gossipsub.unsubscribe(topic: "news")
```
Prefer the async API. Legacy handlers drop any events that arrive before `on` is assigned. Also, core's `Hasher`-based `messageIDFunc`s give different IDs in different processes, so we map them to SHA-256 equivalents (`.hashedSequenceNumberAndFrom` and `.hashedMessage`).

## Contributing

Contributions are welcome! Please file issues, and open pull requests, on [GitHub](https://github.com/swift-libp2p/swift-libp2p-pubsub).

Let's make this code better together! 🤝

## Credits

- [PubSub Spec](https://github.com/libp2p/specs/tree/master/pubsub)
- [The Go PubSub implementation](https://github.com/libp2p/go-libp2p-pubsub)
- [The Rust GossipSub implementation](https://github.com/libp2p/rust-libp2p/tree/master/protocols/gossipsub)
- [The JS PubSub implementation](https://github.com/libp2p/js-libp2p-interfaces)

## License

[MIT](LICENSE.md) © 2026 Breth Inc.
