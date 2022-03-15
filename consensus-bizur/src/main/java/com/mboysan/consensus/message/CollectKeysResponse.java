package com.mboysan.consensus.message;

import java.util.Set;

public class CollectKeysResponse extends KVOperationResponse {

    private final Set<String> keySet;

    public CollectKeysResponse(boolean success, Exception exception, Set<String> keySet) {
        super(success, exception);
        this.keySet = keySet;
    }

    public Set<String> keySet() {
        return keySet;
    }

    @Override
    public String toString() {
        return "CollectKeysResponse{" +
                "keySet=" + keySet +
                "} " + super.toString();
    }
}
