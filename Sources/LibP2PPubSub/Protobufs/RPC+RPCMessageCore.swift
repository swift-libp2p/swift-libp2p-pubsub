//
//  RPC+RPCMessageCore.swift
//  
//
//  Created by Brandon Toms on 4/23/22.
//

import LibP2P

extension RPC.SubOpts:SubOptsCore { }

extension RPC.Message:PubSubMessage { }

extension RPC:RPCMessageCore {
    var subs: [SubOptsCore] {
        self.subscriptions.map { $0 as SubOptsCore }
    }
    
    var messages: [PubSubMessage] {
        self.msgs.map { $0 as PubSubMessage }
    }
}


extension RPC {
    init(_ rpc:RPCMessageCore) throws {
        self.msgs = rpc.messages.map {
            var msg = RPC.Message()
            msg.data = $0.data
            msg.from = $0.from
            msg.seqno = $0.seqno
            msg.topicIds = $0.topicIds
            msg.signature = $0.signature
            msg.key = $0.key
            return msg
        }
        self.subscriptions = rpc.subs.map {
            var sub = RPC.SubOpts()
            sub.subscribe = $0.subscribe
            sub.topicID = $0.topicID
            return sub
        }
    }
}
