package com.mboysan.consensus.message;

public class BizurKVSetResponse extends BizurKVOperationResponse {
    public BizurKVSetResponse(boolean success, Exception exception) {
        super(success, exception);
    }

    @Override
    public String toString() {
        return "BizurKVSetResponse{} " + super.toString();
    }
}
