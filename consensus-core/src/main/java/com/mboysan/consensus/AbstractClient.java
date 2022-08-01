package com.mboysan.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractClient {
    private final Transport transport;
    private final List<Integer> nodeIds;
    private final AtomicInteger currIndex = new AtomicInteger(-1);

    AbstractClient(Transport transport) {
        this.transport = transport;
        this.nodeIds = new ArrayList<>(Objects.requireNonNull(transport.getDestinationNodeIds()));
    }

    Transport getTransport() {
        return transport;
    }

    int nextNodeId() {
        return nodeIds.get(currIndex.incrementAndGet() % nodeIds.size());
    }
}
