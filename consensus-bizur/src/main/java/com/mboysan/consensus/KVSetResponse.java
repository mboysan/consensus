package com.mboysan.consensus;

public class KVSetResponse extends KVOperationResponse {
    public KVSetResponse(boolean success, Exception exception) {
        super(success, exception);
    }

    @Override
    public String toString() {
        return "KVSetResponse{} " + super.toString();
    }
}
