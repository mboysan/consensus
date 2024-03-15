package com.mboysan.consensus.message;

public class BizurKVDeleteResponse extends BizurKVOperationResponse {
    public BizurKVDeleteResponse(boolean success, Exception exception) {
        super(success, exception);
    }

    @Override
    public String toString() {
        return "BizurKVDeleteResponse{} " + super.toString();
    }
}
