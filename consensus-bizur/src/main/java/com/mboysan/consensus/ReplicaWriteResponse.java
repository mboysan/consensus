package com.mboysan.consensus;

public class ReplicaWriteResponse extends Message {
    private final boolean acked;

    public ReplicaWriteResponse(boolean acked) {
        this.acked = acked;
    }

    public boolean isAcked() {
        return acked;
    }

    @Override
    public String toString() {
        return "ReplicaWriteResponse{" +
                "acked=" + acked +
                "} " + super.toString();
    }
}
