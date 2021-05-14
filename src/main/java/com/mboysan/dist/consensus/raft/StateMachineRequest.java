package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.Message;

public class StateMachineRequest extends Message {
    private final String command;

    public StateMachineRequest(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "StateMachineRequest{" +
                "command='" + command + '\'' +
                "} " + super.toString();
    }
}
