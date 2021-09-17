package com.mboysan.consensus;

public class KVSetResponse extends Message {
    private final boolean success;

    public KVSetResponse(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "KVSetResponse{" +
                "success=" + success +
                "} " + super.toString();
    }
}
