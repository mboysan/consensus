package com.mboysan.consensus;

public class KVOperationResponse extends Message {
    private final boolean success;
    private final Exception exception;

    public KVOperationResponse(boolean success, Exception exception) {
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
        return "KVOperationResponse{" +
                "success=" + success +
                ", exception=" + exception +
                "} " + super.toString();
    }
}
