package com.mboysan.consensus.message;

public class CheckStoreIntegrityResponse extends Message {
    private final boolean success;
    private final String protocol;
    private final String integrityHash;
    private final String state;
    private final Exception exception;

    public CheckStoreIntegrityResponse(Exception exception, String protocol) {
        this.success = false;
        this.protocol = protocol;
        this.integrityHash = null;
        this.state = null;
        this.exception = exception;
    }

    public CheckStoreIntegrityResponse(boolean success, String protocol, String integrityHash, String state) {
        this.success = success;
        this.protocol = protocol;
        this.integrityHash = integrityHash;
        this.state = state;
        this.exception = null;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getIntegrityHash() {
        return integrityHash;
    }

    public String getState() {
        return state;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "CheckStoreIntegrityResponse{" +
                "success=" + success +
                ", protocol='" + protocol + '\'' +
                ", integrityHash='" + integrityHash + '\'' +
                ", state='" + state + '\'' +
                ", exception=" + exception +
                "} " + super.toString();
    }
}
