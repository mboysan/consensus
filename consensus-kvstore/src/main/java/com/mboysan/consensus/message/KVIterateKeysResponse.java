package com.mboysan.consensus.message;

import java.util.Set;

public class KVIterateKeysResponse extends KVOperationResponse {
    private final Set<String> keys;

    public KVIterateKeysResponse(boolean success, Exception exception, Set<String> keys) {
        super(success, exception);
        this.keys = keys;
    }

    public Set<String> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "KVIterateKeysResponse{" +
                "keys=" + keys +
                "} " + super.toString();
    }
}
