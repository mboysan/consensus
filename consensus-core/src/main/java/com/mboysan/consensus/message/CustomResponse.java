package com.mboysan.consensus.message;

public class CustomResponse extends Message {

    private final boolean success;

    private final Exception exception;
    private final String payload;

    public CustomResponse(boolean success, Exception exception, String payload) {
        this.success = success;
        this.exception = exception;
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "CustomResponse{" +
                "success=" + success +
                ", exception=" + exception +
                ", payload='" + payload + '\'' +
                "} " + super.toString();
    }
}
