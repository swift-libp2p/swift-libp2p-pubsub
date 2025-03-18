//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2025 swift-libp2p project authors
// Licensed under MIT
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of swift-libp2p project authors
//
// SPDX-License-Identifier: MIT
//
//===----------------------------------------------------------------------===//

import LibP2P
import LibP2PMPLEX
import LibP2PNoise
import XCTest

@testable import LibP2PPubSub

class LibP2PPubSubGossipsubTests: XCTestCase {

    /// **************************************
    ///     Testing Internal Gossipsub
    /// **************************************
    func testLibP2PPubSub_GossipSub() throws {
        /// Init the libp2p nodes
        let node1 = try makeHost()
        let node2 = try makeHost()

        /// Prepare our expectations
        let expectationNode1ReceivedNode2Subscription = expectation(
            description: "Node1 received fruit subscription from Node2"
        )
        let expectationNode1ReceivedNode2Message = expectation(description: "Node1 received message from Node2")
        let expectationNode2ReceivedNode1Subscription = expectation(
            description: "Node2 received fruit subscription from Node1"
        )
        let expectationNode2ReceivedNode1Message = expectation(description: "Node2 received message from Node1")

        let node1Message = "banana"
        let node2Message = "pineapple"

        /// Node1 subscribes to topic 'fruit'
        let subscription1 = try node1.pubsub.gossipsub.subscribe(
            .init(
                topic: "fruit",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        subscription1.on = { event -> EventLoopFuture<Void> in
            switch event {
            case .newPeer(let peer):
                node1.logger.info("Node1::NewPeer -> \(peer)")
                XCTAssertEqual(peer, node2.peerID)
                expectationNode1ReceivedNode2Subscription.fulfill()

            case .data(let pubSubMessage):
                node1.logger.info("Node1 -> \(String(data: pubSubMessage.data, encoding: .utf8) ?? "NIL")")
                XCTAssertEqual(String(data: pubSubMessage.data, encoding: .utf8), node2Message)
                expectationNode1ReceivedNode2Message.fulfill()

            case .error(let error):
                node1.logger.error("Node1 Error: \(error)")
                XCTFail(error.localizedDescription)
            }
            return node1.eventLoopGroup.next().makeSucceededVoidFuture()
        }

        /// Node2 subcribes to topic 'fruit'
        //let subscription2 = try fSub2.subscribe(topic: "fruit")
        let subscription2 = try node2.pubsub.gossipsub.subscribe(
            .init(
                topic: "fruit",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        subscription2.on = { event -> EventLoopFuture<Void> in
            switch event {
            case .newPeer(let peer):
                node2.logger.info("Node2::NewPeer -> \(peer)")
                XCTAssertEqual(peer, node1.peerID)
                expectationNode2ReceivedNode1Subscription.fulfill()

            case .data(let pubSubMessage):
                node2.logger.info("Node2 -> \(String(data: pubSubMessage.data, encoding: .utf8) ?? "NIL")")
                XCTAssertEqual(String(data: pubSubMessage.data, encoding: .utf8), node1Message)
                expectationNode2ReceivedNode1Message.fulfill()

            case .error(let error):
                node2.logger.error("Node2 Error: \(error)")
                XCTFail(error.localizedDescription)
            }
            return node2.eventLoopGroup.next().makeSucceededVoidFuture()
        }

        /// Start the libp2p nodes
        try node1.start()
        try node2.start()

        sleep(1)

        /// Have node1 reach out to node2
        try node1.newStream(to: node2.listenAddresses.first!, forProtocol: "/meshsub/1.0.0")

        /// Publish some messages...
        node1.eventLoopGroup.next().scheduleTask(in: .seconds(2)) {
            subscription1.publish(node1Message.data(using: .utf8)!)
        }
        node2.eventLoopGroup.next().scheduleTask(in: .seconds(3)) {
            subscription2.publish(node2Message.data(using: .utf8)!)
        }

        waitForExpectations(timeout: 10, handler: nil)

        subscription1.unsubscribe()
        subscription2.unsubscribe()

        sleep(1)

        /// Check to see if we can poll our PeerStore for known peers that support '/chat/1.0.0'
        let peers = try node1.peers.getPeers(supportingProtocol: SemVerProtocol("/meshsub/1.0.0")!, on: nil).wait()
        XCTAssertEqual(peers.count, 1)
        XCTAssertEqual(peers.first!, node2.peerID.b58String)

        /// Dump the current state of our PeerStore
        node1.peers.dumpAll()

        node1.pubsub.gossipsub.dumpEventList()

        node2.pubsub.gossipsub.dumpEventList()

        /// Stop the nodes
        node1.shutdown()
        node2.shutdown()

        print("All Done!")
    }

    /// **************************************
    ///     Testing Internal Gossipsub Subscriptions
    /// **************************************
    /// Node 1 publishes a continous stream of news
    /// Node 2 Subscribes / Unsubscribes periodically
    /// We assert Node 1 stops sending messages while Node 2 is unsubscribed
    /// We ensure Node 1 updates the subscription changes appropriately
    func testLibP2PPubSub_Gossipsub_Subscriptions() throws {
        /// Init the libp2p nodes
        let node1 = try makeHost()
        let node2 = try makeHost()

        /// Prepare our expectations
        let expectationNode1ReceivedNode2Subscription = expectation(
            description: "Node1 received fruit subscription from Node2"
        )
        //let expectationNode1ReceivedNode2Unsubscription = expectation(description: "Node1 received fruit unsubscription from Node2")
        let expectationNode1ReceivedNode2SecondSubscription = expectation(
            description: "Node1 received fruit subscription from Node2 for the second time"
        )

        let expectationNode2ReceivedNode1Subscription = expectation(
            description: "Node2 received fruit subscription from Node1"
        )
        let expectationNode2ReceivedFirstNode1Message = expectation(
            description: "Node2 received first message from Node1"
        )
        let expectationNode2ReceivedSecondNode1Message = expectation(
            description: "Node2 received first message from Node1"
        )

        let node1Message = "hot news!"
        var node2SubscriptionCount = 0
        /// Node1 subscribes to topic 'fruit'
        let subscription1 = try node1.pubsub.gossipsub.subscribe(
            .init(
                topic: "news",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .hashSequenceNumberAndFromFields
            )
        )
        subscription1.on = { event -> EventLoopFuture<Void> in
            switch event {
            case .newPeer(let peer):
                node1.logger.info("Node1::NewPeer -> \(peer)")
                XCTAssertEqual(peer, node2.peerID)
                node2SubscriptionCount += 1
                if node2SubscriptionCount == 1 {
                    expectationNode1ReceivedNode2Subscription.fulfill()
                } else if node2SubscriptionCount == 2 {
                    expectationNode1ReceivedNode2SecondSubscription.fulfill()
                }

            case .data(let pubSubMessage):
                node1.logger.info("Node1 -> \(pubSubMessage)")
                XCTFail("Node 1 shouldn't receive data during this test")

            case .error(let error):
                node1.logger.error("Node1 Error: \(error)")
                XCTFail(error.localizedDescription)
            }
            return node1.eventLoopGroup.next().makeSucceededVoidFuture()
        }

        /// Node2 subcribes to topic 'fruit'
        var node2Messages: [PubSubMessage] = []
        var fulfillmentCount = 0
        let subscriptionHandler: (PubSub.SubscriptionEvent) -> EventLoopFuture<Void> = {
            event -> EventLoopFuture<Void> in
            switch event {
            case .newPeer(let peer):
                node2.logger.info("Node2::NewPeer -> \(peer)")
                XCTAssertEqual(peer, node1.peerID)
                expectationNode2ReceivedNode1Subscription.fulfill()

            case .data(let pubSubMessage):
                node2.logger.info("Node2 -> \(String(data: pubSubMessage.data, encoding: .utf8) ?? "NIL")")
                //XCTAssertEqual(String(data: pubSubMessage.data, encoding: .utf8), node1Message)
                node2Messages.append(pubSubMessage)
                if fulfillmentCount == 0 {
                    expectationNode2ReceivedFirstNode1Message.fulfill()
                    fulfillmentCount += 1
                } else if fulfillmentCount == 1 {
                    expectationNode2ReceivedSecondNode1Message.fulfill()
                    fulfillmentCount += 1
                }

            case .error(let error):
                node2.logger.error("Node2 Error: \(error)")
                XCTFail(error.localizedDescription)
            }
            return node2.eventLoopGroup.next().makeSucceededVoidFuture()
        }

        var subscription2 = try node2.pubsub.gossipsub.subscribe(
            .init(
                topic: "news",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .hashSequenceNumberAndFromFields
            )
        )
        subscription2.on = subscriptionHandler

        /// Start the libp2p nodes
        try node1.start()
        try node2.start()

        sleep(1)

        /// Have node1 reach out to node2
        try node2.newStream(to: node1.listenAddresses.first!, forProtocol: GossipSub.multicodec)

        /// Publish some messages...
        var counter = 0
        var node1Messages: [String] = []
        let task = node1.eventLoopGroup.next().scheduleRepeatedTask(
            initialDelay: .milliseconds(500),
            delay: .seconds(1)
        ) { task in
            let msg = "\(node1Message)[\(counter)]"
            subscription1.publish(msg.data(using: .utf8)!)
            node1Messages.append(msg)
            counter += 1
        }

        /// Wait for initial subscription alerts and the first message to arrive on Node 2
        wait(
            for: [
                expectationNode1ReceivedNode2Subscription, expectationNode2ReceivedNode1Subscription,
                expectationNode2ReceivedFirstNode1Message,
            ],
            timeout: 10,
            enforceOrder: false
        )

        //sleep(1)

        /// Unsubscribe Node2 from our `news` subscription
        //try node2.pubsub.gossipsub.unsubscribe(topic: "news").wait()
        subscription2.unsubscribe()

        //wait(for: [expectationNode1ReceivedNode2Unsubscription], timeout: 10, enforceOrder: false)

        sleep(2)

        /// Re subscribe Node2 to our `news` subscription
        subscription2 = try node2.pubsub.gossipsub.subscribe(
            .init(
                topic: "news",
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .hashSequenceNumberAndFromFields
            )
        )
        subscription2.on = subscriptionHandler

        /// Wait for the second subscription alert on Node1 and the second `news` message to arrive at Node2
        wait(
            for: [expectationNode1ReceivedNode2SecondSubscription, expectationNode2ReceivedSecondNode1Message],
            timeout: 10,
            enforceOrder: false
        )

        /// Stop sending messages
        task.cancel()

        // Wait a couple senconds
        sleep(2)

        //try node2.pubsub.gossipsub.unsubscribe(topic: "news").wait()
        subscription2.unsubscribe()

        sleep(1)

        /// Check to see if we can poll our PeerStore for known peers that support '/chat/1.0.0'
        let peers = try node1.peers.getPeers(supportingProtocol: SemVerProtocol(GossipSub.multicodec)!, on: nil).wait()
        XCTAssertEqual(peers.count, 1)
        XCTAssertEqual(peers.first!, node2.peerID.b58String)

        /// Ensure Node1 Subscription count equals 2 (Node2 subscribed twice)
        XCTAssertEqual(node2SubscriptionCount, 2)
        /// Ensure the Node2 received the appropriate number of `news` messages
        //XCTAssertGreaterThanOrEqual(node2Messages.count, messagesPerBatch * 2)
        XCTAssertGreaterThanOrEqual(node2Messages.count, 2)
        XCTAssertLessThanOrEqual(node2Messages.count, node1Messages.count)

        print("Node 1 Sent Messages")
        print(node1Messages)
        print("Node 2 Received Messages")
        print(node2Messages.map { String(data: $0.data, encoding: .utf8) ?? "NIL" }.joined(separator: "\n"))

        /// Stop the nodes
        node1.shutdown()
        node2.shutdown()

        print("All Done!")
    }

    /// **************************************
    ///  Testing Gossipsub Message Propogation
    /// **************************************
    /// This test launches the specified number of nodes, registers gossipsub and subscribes to the 'fruit' topic.
    /// We then itterate through the nodes and connect each one to exactly one other using various structures
    /// 1) Linear chain grouping - each node knows about the node ahead of it in our array and thats it (messages are propogated through the array in a linear fashion)
    /// 2) Beacon Grouping - each node is linked to the first node (index 0) and only the first node. (messages are beaconed out of the main node)
    /// 3) Beacon2Beacon: Dense Multi Cluster with sparse connections between them. (10 nodes, first 5 are tightly interconnected, second 5 are tightly interconnected, node 0 and 5 bridge the clusters)
    /// Each node then publishes a message and we wait / ensure that all nodes subscribed to the topic receive all messages.
    func testLibP2PPubSub_Gossipsub_NNodes() throws {

        enum NetworkStructure {
            case linear
            case circular
            case beacon
            case beacon2beacon
        }

        class Node {
            let libp2p: Application
            let expectation: XCTestExpectation
            let messageToSend: String
            var messagesReceived: [String]
            var handler: PubSub.SubscriptionHandler!

            init(libp2p: Application, expectation: XCTestExpectation, messageToSend: String) {
                self.libp2p = libp2p
                self.expectation = expectation
                self.messageToSend = messageToSend
                self.messagesReceived = [messageToSend]
                self.handler = nil
            }
        }

        /// Consider the ConenctionManagers max concurrent connections param while setting this number (especially for the beacon structure) (the default is 25 connections)
        let nodesToTest: Int = 10
        let structureToTest: NetworkStructure = .beacon

        //guard nodesToTest > 2 else { XCTFail("We need at least 3 nodes to accurately perform this test..."); return }

        /// Init the libp2p nodes, gossipsub routers, and prepare our expectations
        var nodes: [Node] = try (0..<nodesToTest).map { idx in
            let node = try makeHost()
            return Node(
                libp2p: node,
                expectation: expectation(description: "Node\(idx) received all messages"),
                messageToSend: "Hello from node\(idx) 🍌"
            )
        }

        /// For each node, subscribe to the topic 'fruit' and register our on 'fruit' subscription handler...
        for node in nodes {
            /// Subscribe to topic using a SubscriptionConfiguration (this lets us define our subscription behavior on a per topic level)
            node.handler = try node.libp2p.pubsub.gossipsub.subscribe(
                PubSub.SubscriptionConfig(
                    topic: "fruit",
                    signaturePolicy: .strictSign,
                    validator: .acceptAll,
                    messageIDFunc: .hashSequenceNumberAndFromFields
                )
            )
            node.handler.on = { event -> EventLoopFuture<Void> in
                switch event {
                case .newPeer(let peer):
                    node.libp2p.logger.debug("Node[\(node.libp2p.peerID)]::NewPeer -> \(peer)")

                case .data(let pubSubMessage):
                    node.libp2p.logger.debug("Node[\(node.libp2p.peerID)]::Data -> \(pubSubMessage)")
                    node.messagesReceived.append(String(data: pubSubMessage.data, encoding: .utf8)!)
                    if node.messagesReceived.count == nodesToTest {
                        node.expectation.fulfill()
                    }

                case .error(let error):
                    node.libp2p.logger.error("Node[\(node.libp2p.peerID)]::Error -> \(error)")
                    XCTFail(error.localizedDescription)
                }
                return node.libp2p.eventLoopGroup.next().makeSucceededVoidFuture()
            }
        }

        /// Start the libp2p nodes
        for node in nodes { try node.libp2p.start() }

        /// ******************************************
        /// The following logic determines the structure of the network
        /// ******************************************
        switch structureToTest {
        case .linear:
            /// Have each node reach out to the next node in our array... (a linear chain set up)
            ///
            /// Network Structure Diagram
            ///  n -> ... -> n
            for (idx, node) in nodes.enumerated() {
                guard nodes.count > (idx + 1) else { continue }
                try node.libp2p.newStream(
                    to: nodes[idx + 1].libp2p.listenAddresses.first!,
                    forProtocol: GossipSub.multicodec
                )
            }

        case .circular:
            /// If we tie the ends together, we have a circular network graph
            /// - Note: with 3 Nodes, this results in 6 wasted / redundant message propogations, 7 Nodes -> 14 redundant messages...
            /// Network Structure Diagram
            ///  n -> ... -> n -,
            ///  ^                |
            ///  '--------------'
            for (idx, node) in nodes.enumerated() {
                guard nodes.count > (idx + 1) else {
                    try node.libp2p.newStream(
                        to: nodes[0].libp2p.listenAddresses.first!,
                        forProtocol: GossipSub.multicodec
                    )
                    continue
                }
                try node.libp2p.newStream(
                    to: nodes[idx + 1].libp2p.listenAddresses.first!,
                    forProtocol: GossipSub.multicodec
                )
            }

        case .beacon:
            /// Have each node reach out to the zeroeth node (a beacon set up)
            ///
            /// Network Structure Diagram
            ///  n
            ///  : \
            ///     -- n
            ///  : /
            ///  n
            ///
            for (idx, node) in nodes.enumerated() {
                guard idx != 0 else { continue }
                try node.libp2p.newStream(to: nodes[0].libp2p.listenAddresses.first!, forProtocol: GossipSub.multicodec)
            }

        case .beacon2beacon:
            /// Splits the network into Evens & Odds then connects node 0 and 1 to bridge the devide...
            ///
            /// Network Structure Diagram
            ///  n              n
            ///    \          /
            ///  n -- n -- n -- n
            ///    /          \
            ///  n              n
            ///
            for (idx, node) in nodes.enumerated() {
                guard idx != 0 else { continue }
                if idx == 1 {
                    /// Have Node1 reach out to Node0
                    try node.libp2p.newStream(
                        to: nodes[0].libp2p.listenAddresses.first!,
                        forProtocol: GossipSub.multicodec
                    )
                    continue
                }
                if idx % 2 == 0 {
                    /// If the node is an even number (have it reach out to Node0, our even beacon node)
                    try node.libp2p.newStream(
                        to: nodes[0].libp2p.listenAddresses.first!,
                        forProtocol: GossipSub.multicodec
                    )
                } else {
                    /// Otherwise the node must be odd (have it reach out to Node1, our odd beacon node)
                    try node.libp2p.newStream(
                        to: nodes[1].libp2p.listenAddresses.first!,
                        forProtocol: GossipSub.multicodec
                    )
                }
            }

        }

        /// Publish some messages...
        for node in nodes {
            node.libp2p.eventLoopGroup.next().scheduleTask(in: .milliseconds(Int64.random(in: 500...3_000))) {
                //node.5!.publish(node.3.data(using: .utf8)!)
                node.libp2p.pubsub.publish(node.messageToSend.data(using: .utf8)!.bytes, toTopic: "fruit")
            }
        }

        /// Wait for each node to receive each message
        waitForExpectations(timeout: 10, handler: nil)

        /// Wait an additional 2 seconds to ensure message propogation doesn't echo through the network causing duplicates
        sleep(1)

        nodes.first!.libp2p.peers.dumpAll()
        nodes.first!.libp2p.pubsub.gossipsub.dumpEventList()

        /// Stop the nodes
        for node in nodes { node.libp2p.shutdown() }

        /// Ensure that each node received every message...
        for node in nodes {
            XCTAssertEqual(node.messagesReceived.count, nodes.count)
            for i in (0..<nodesToTest) {
                XCTAssertTrue(node.messagesReceived.contains("Hello from node\(i) 🍌"))
            }
            print("Node[\(node.libp2p.peerID)] Messages Received:")
            print(node.messagesReceived)
        }

        /// Remove all nodes and ensure memory is released....
        nodes.removeAll()

        sleep(1)

        print("All Done!")
    }

    /// **************************************
    ///      Testing JS Interoperability
    /// **************************************
    /// In order to run this example, launch the js-libp2p/examples/pubsub example then connect to one of the two peers that example spawns
    /// - Note: Using a shell / terminal window execute the following commands...
    /// ```
    /// git clone https://github.com/libp2p/js-libp2p.git //if you dont have the js-libp2p repo yet
    /// cd js-libp2p
    /// npm install
    /// cd examples
    /// npm install
    /// cd pubsub
    /// node 1.js
    /// // copy the port number of the peer and place it in the `newStream` call below
    /// ```
    /// - Note: The JS example uses the `concatFromAndSequenceFields` messageID function
    func testGossipsubJSInterop() throws {
        throw XCTSkip("Integration Test Skipped By Default")
        let app = try Application(.testing, peerID: PeerID(.Ed25519))
        app.logger.logLevel = .trace

        /// Configure our networking stack!
        app.servers.use(.tcp(host: "127.0.0.1", port: 10000))
        app.security.use(.noise)
        app.muxers.use(.mplex)
        app.pubsub.use(.gossipsub)

        let topic = "news"
        var expectedMessageCount = 4
        let messageExpectation = expectation(description: "MessagesReceived")

        try app.start()
        try app.pubsub.gossipsub.start()

        let subscription = try app.pubsub.gossipsub.subscribe(
            .init(
                topic: topic,
                signaturePolicy: .strictSign,
                validator: .acceptAll,
                messageIDFunc: .concatFromAndSequenceFields
            )
        )
        subscription.on = { event -> EventLoopFuture<Void> in
            switch event {
            case .newPeer(let peer):
                print("[\(peer)]: Subscribed to \(topic)!")
                app.eventLoopGroup.next().scheduleTask(in: .milliseconds(100)) {
                    app.pubsub.publish("Hello from swift!".data(using: .utf8)!.bytes, toTopic: topic)
                }

            case .data(let pubSubMessage):
                print(String(data: pubSubMessage.data, encoding: .utf8) ?? "NIL")
                expectedMessageCount -= 1
                if expectedMessageCount == 0 {
                    messageExpectation.fulfill()
                }

            case .error(let error):
                app.logger.error("Error: \(error)")
            }
            return app.eventLoopGroup.next().makeSucceededVoidFuture()
        }

        try? app.newStream(to: Multiaddr("/ip4/192.168.1.19/tcp/56758"), forProtocol: "/ipfs/ping/1.0.0")

        waitForExpectations(timeout: 30)

        let _ = app.pubsub.publish("Goodbyte from swift!".data(using: .utf8)!.bytes, toTopic: topic)
        //subscription.publish("Goodbyte from swift!".data(using: .utf8)!.bytes)

        subscription.unsubscribe()

        sleep(1)

        print("Shutting down libp2p chat...")
        app.peers.dumpAll()

        app.pubsub.gossipsub.dumpEventList()

        try app.pubsub.gossipsub.stop()
        app.running?.stop()
        app.shutdown()
    }

    var nextPort: Int = 10100
    private func makeHost() throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519))
        lib.logger.logLevel = .info
        lib.connectionManager.use(connectionType: BasicConnectionLight.self)
        lib.security.use(.noise)
        lib.muxers.use(.mplex)
        lib.pubsub.use(.gossipsub)
        lib.servers.use(.tcp(host: "127.0.0.1", port: nextPort))

        nextPort += 1

        return lib
    }
}
