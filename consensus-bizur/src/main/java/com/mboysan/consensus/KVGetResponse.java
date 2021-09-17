package com.mboysan.consensus;

public class KVGetResponse extends Message {
    private final boolean success;
    private final String value;

    public KVGetResponse(boolean success, String value) {
        this.success = success;
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KVGetResponse{" +
                "success=" + success +
                ", value='" + value + '\'' +
                "} " + super.toString();
    }
}
