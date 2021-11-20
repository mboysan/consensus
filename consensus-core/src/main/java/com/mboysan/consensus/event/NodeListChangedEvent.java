package com.mboysan.consensus.event;

import java.util.Set;

public class NodeListChangedEvent implements IEvent {
    private final int targetNodeId;
    private final Set<Integer> serverIds;

    public NodeListChangedEvent(int targetNodeId, Set<Integer> serverIds) {
        this.targetNodeId = targetNodeId;
        this.serverIds = serverIds;
    }

    public int getTargetNodeId() {
        return targetNodeId;
    }

    public Set<Integer> getServerIds() {
        return serverIds;
    }

    @Override
    public String toString() {
        return "NodeListChangedEvent{" +
                "targetNodeId=" + targetNodeId +
                ", serverIds=" + serverIds +
                '}';
    }
}
