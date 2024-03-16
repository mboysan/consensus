package com.mboysan.consensus.message;

public class BizurKVGetResponse extends BizurKVOperationResponse {
    private final String value;

    public BizurKVGetResponse(boolean success, Exception exception, String value) {
        super(success, exception);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "BizurKVGetResponse{" +
                "value='" + value + '\'' +
                "} " + super.toString();
    }
}
