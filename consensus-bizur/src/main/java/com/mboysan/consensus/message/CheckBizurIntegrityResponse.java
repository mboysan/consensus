package com.mboysan.consensus.message;

public class CheckBizurIntegrityResponse extends Message {

    private final boolean success;
    private final String integrityHash;
    private final String state;

    public CheckBizurIntegrityResponse(boolean success, String integrityHash, String state) {
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
        return "CheckBizurIntegrityResponse{" +
                "success=" + success +
                ", integrityHash='" + integrityHash + '\'' +
                ", state='" + state + '\'' +
                ", protocol='bizur'"+
                "} " + super.toString();
    }
}
