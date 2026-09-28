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

extension RPC.SubOpts: SubOptsCore {}

extension RPC.Message: PubSubMessage {}

extension RPC: RPCMessageCore {
    var subs: [SubOptsCore] {
        self.subscriptions.map { $0 as SubOptsCore }
    }

    var messages: [PubSubMessage] {
        self.msgs.map { $0 as PubSubMessage }
    }
}
