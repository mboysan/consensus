package com.mboysan.consensus.time;

public class UnixSecondsTimestamp implements Timestamp {

    private static final long TO_SECONDS = 1000L;

    private final long timestamp;

    public UnixSecondsTimestamp() {
        this(System.currentTimeMillis());
    }

    public UnixSecondsTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
        return timestamp / TO_SECONDS;
    }
}
