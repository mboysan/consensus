package com.mboysan.consensus.message;

import java.util.Set;

public class BizurKVIterateKeysResponse extends BizurKVOperationResponse {
    private final Set<String> keys;

    public BizurKVIterateKeysResponse(boolean success, Exception exception, Set<String> keys) {
        super(success, exception);
        this.keys = keys;
    }

    public Set<String> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "BizurKVIterateKeysResponse{" +
                "keys=" + keys +
                "} " + super.toString();
    }
}
