package com.mboysan.consensus;

import java.util.concurrent.atomic.AtomicInteger;

class SimState {
    private final Role role;

    SimState(Role role) {
        this.role = role;
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

    @Override
    public String toString() {
        return "SimState{" +
                "role=" + role +
                ", messageReceiveCount=" + messageReceiveCount.get() +
                '}';
    }
}
