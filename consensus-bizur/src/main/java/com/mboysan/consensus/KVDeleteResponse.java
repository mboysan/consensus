package com.mboysan.consensus;

public class KVDeleteResponse extends KVOperationResponse {
    public KVDeleteResponse(boolean success, Exception exception) {
        super(success, exception);
    }

    @Override
    public String toString() {
        return "KVDeleteResponse{} " + super.toString();
    }
}
