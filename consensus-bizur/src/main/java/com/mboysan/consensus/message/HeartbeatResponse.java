package com.mboysan.consensus.message;

public class HeartbeatResponse extends Message {
    private final long sendTimeMs;

    public HeartbeatResponse(long sendTimeMs) {
        this.sendTimeMs = sendTimeMs;
    }

    public long getSendTimeMs() {
        return sendTimeMs;
    }

    @Override
    public String toString() {
        return "HeartbeatResponse{" +
                "sendTimeMs=" + sendTimeMs +
                "} " + super.toString();
    }
}
