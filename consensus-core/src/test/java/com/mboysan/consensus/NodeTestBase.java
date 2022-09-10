package com.mboysan.consensus;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

abstract class NodeTestBase {

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
