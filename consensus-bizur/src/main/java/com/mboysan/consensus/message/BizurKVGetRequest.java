package com.mboysan.consensus.message;

public class BizurKVGetRequest extends Message {
    private final String key;

    public BizurKVGetRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "BizurKVGetRequest{" +
                "key='" + key + '\'' +
                "} " + super.toString();
    }
}
