package com.mboysan.consensus.event;

public class NodeStoppedEvent implements IEvent {
    private final int sourceNodeId;

    public NodeStoppedEvent(int sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public int getSourceNodeId() {
        return sourceNodeId;
    }
}
