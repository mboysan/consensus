package com.mboysan.consensus.message;

public class CheckRaftIntegrityResponse extends Message {

    private final boolean success;
    private final String integrityHash;
    private final String state;

    public CheckRaftIntegrityResponse(boolean success, String integrityHash, String state) {
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
        return "CheckRaftIntegrityResponse{" +
                "success=" + success +
                ", integrityHash='" + integrityHash + '\'' +
                ", state='" + state + '\'' +
                "} " + super.toString();
    }
}
