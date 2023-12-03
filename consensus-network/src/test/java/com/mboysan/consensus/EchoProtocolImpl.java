package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;

import static com.mboysan.consensus.util.AwaitUtil.doSleep;

public class EchoProtocolImpl implements RPCProtocol {
    public static final String PAYLOAD_WAIT = "wait";

    @Override
    public Message processRequest(Message request) {
        if (!(request instanceof TestMessage req)) {
            throw new IllegalArgumentException("unsupported message type");
        }

        if (PAYLOAD_WAIT.equals(req.getPayload())) {
            // wait for some time and respond afterward.
            doSleep(30000);    // 30 secs.
        }

        return new TestMessage(req.getPayload());
    }
}
