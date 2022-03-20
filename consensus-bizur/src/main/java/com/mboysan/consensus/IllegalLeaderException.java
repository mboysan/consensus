package com.mboysan.consensus;

public class IllegalLeaderException extends BizurException {
    private final int leaderId;

    public IllegalLeaderException(int leaderId) {
        super(null);
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }
}
