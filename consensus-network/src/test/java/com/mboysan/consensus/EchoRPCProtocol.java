package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EchoRPCProtocol implements RPCProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(EchoRPCProtocol.class);

    @Override
    public Message processRequest(Message request) {
        if (!(request instanceof TestMessage req)) {
            throw new IllegalArgumentException("unsupported message type");
        }

        if ("wait".equals(req.getPayload())) {
            try {
                // wait for some time and respond afterwards.
                Thread.sleep(30000);    // 30 secs.
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        return new TestMessage(req.getPayload()).responseTo(req);
    }
}
