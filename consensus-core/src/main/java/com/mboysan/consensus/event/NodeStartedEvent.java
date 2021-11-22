package com.mboysan.consensus.event;

public class NodeStartedEvent implements IEvent {
    private final int sourceNodeId;

    public NodeStartedEvent(int sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public int getSourceNodeId() {
        return sourceNodeId;
    }
}
