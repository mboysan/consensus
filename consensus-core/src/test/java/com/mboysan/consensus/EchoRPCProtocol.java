package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;

import java.util.Set;

public class EchoRPCProtocol implements RPCProtocol {
    @Override
    public void onNodeListChanged(Set<Integer> serverIds) {
        // do nothing
    }

    @Override
    public Message processRequest(Message request) {
        if (!(request instanceof TestMessage)) {
            throw new IllegalArgumentException("unsupported message type");
        }
        TestMessage req = (TestMessage) request;
        return new TestMessage(req.getPayload()).responseTo(req);
    }
}
