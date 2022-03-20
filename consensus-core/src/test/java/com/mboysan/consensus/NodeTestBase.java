package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Configuration;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

abstract class NodeTestBase {

    static {
        Properties properties = new Properties();
        properties.put("transport.message.callbackTimeoutMs", 100 + "");
        Configuration.getCached(Configuration.class, properties); // InVMTransport's callbackTimeout will be overridden
    }

    abstract InVMTransport getTransport();
    abstract AbstractNode<?> getNode(int nodeId);

    // ------------------------------------------------------------- node kill & revive operations

    void kill(int nodeId) {
        getNode(nodeId).shutdown();
    }

    void revive(int nodeId) throws IOException, ExecutionException, InterruptedException {
        getNode(nodeId).start().get();
    }

    void disconnect(int nodeId) {
        getTransport().connectedToNetwork(nodeId, false);
    }

    void connect(int nodeId) {
        getTransport().connectedToNetwork(nodeId, true);
    }
}
