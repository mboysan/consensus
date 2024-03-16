package com.mboysan.consensus.message;

public class BizurKVOperationResponse extends Message {
    private final boolean success;
    private final Exception exception;

    public BizurKVOperationResponse(boolean success, Exception exception) {
        this.success = success;
        this.exception = exception;
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "BizurKVOperationResponse{" +
                "success=" + success +
                ", exception=" + exception +
                "} " + super.toString();
    }
}
