package com.mboysan.consensus;

import java.util.concurrent.atomic.AtomicInteger;

class SimState {
    private final Role role;

    private final int leaderId;
    private final int nodeId;

    SimState(final int nodeId, final int leaderId) {
        this.nodeId = nodeId;
        this.leaderId = leaderId;
        this.role = nodeId == leaderId ? Role.LEADER : Role.FOLLOWER;
    }

    enum Role {
        FOLLOWER, LEADER
    }

    private final AtomicInteger messageReceiveCount = new AtomicInteger(0);

    public AtomicInteger getMessageReceiveCount() {
        return messageReceiveCount;
    }

    public Role getRole() {
        return role;
    }

    public int getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "SimState{" +
                "role=" + role +
                ", leaderId=" + leaderId +
                ", nodeId=" + nodeId +
                ", messageReceiveCount=" + messageReceiveCount +
                '}';
    }
}
