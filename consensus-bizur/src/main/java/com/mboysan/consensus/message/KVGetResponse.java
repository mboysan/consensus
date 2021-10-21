package com.mboysan.consensus.message;

public class KVGetResponse extends KVOperationResponse {
    private final String value;

    public KVGetResponse(boolean success, Exception exception, String value) {
        super(success, exception);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KVGetResponse{" +
                "value='" + value + '\'' +
                "} " + super.toString();
    }
}
