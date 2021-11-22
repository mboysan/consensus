package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;

public class EchoRPCProtocol implements RPCProtocol {
    @Override
    public Message processRequest(Message request) {
        if (!(request instanceof TestMessage)) {
            throw new IllegalArgumentException("unsupported message type");
        }
        TestMessage req = (TestMessage) request;
        return new TestMessage(req.getPayload()).responseTo(req);
    }
}
