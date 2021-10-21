package com.mboysan.consensus.message;

public class StateMachineResponse extends Message {
    private final boolean isApplied;

    public StateMachineResponse(boolean isApplied) {
        this.isApplied = isApplied;
    }

    public boolean isApplied() {
        return isApplied;
    }

    @Override
    public String toString() {
        return "StateMachineResponse{" +
                "isApplied=" + isApplied +
                "} " + super.toString();
    }
}
