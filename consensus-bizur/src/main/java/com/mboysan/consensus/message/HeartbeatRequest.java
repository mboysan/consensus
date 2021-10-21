package com.mboysan.consensus.message;

public class HeartbeatRequest extends Message {
    private final long sendTimeMs;

    public HeartbeatRequest(long sendTimeMs) {
        this.sendTimeMs = sendTimeMs;
    }

    public long getSendTimeMs() {
        return sendTimeMs;
    }

    @Override
    public String toString() {
        return "HeartbeatRequest{" +
                "sendTimeMs=" + sendTimeMs +
                "} " + super.toString();
    }
}
