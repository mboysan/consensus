package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;

public class EchoRPCProtocol implements RPCProtocol {
    @Override
    public Message processRequest(Message request) {
        if (!(request instanceof TestMessage req)) {
            throw new IllegalArgumentException("unsupported message type");
        }

        if ("wait-forever".equals(req.getPayload())) {
            try {
                // wait indefinitely
                Thread.sleep(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        return new TestMessage(req.getPayload()).responseTo(req);
    }
}
