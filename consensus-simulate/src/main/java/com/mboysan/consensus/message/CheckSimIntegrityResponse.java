package com.mboysan.consensus.message;

public class CheckSimIntegrityResponse extends Message {

    private final boolean success;
    private final String integrityHash;
    private final String state;

    public CheckSimIntegrityResponse(boolean success, String integrityHash, String state) {
        this.success = success;
        this.integrityHash = integrityHash;
        this.state = state;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getIntegrityHash() {
        return integrityHash;
    }

    public String getState() {
        return state;
    }

    @Override
    public String toString() {
        return "CheckSimIntegrityResponse{" +
                "success=" + success +
                ", integrityHash='" + integrityHash + '\'' +
                ", state='" + state + '\'' +
                ", protocol='simulate'"+
                "} " + super.toString();
    }
}
