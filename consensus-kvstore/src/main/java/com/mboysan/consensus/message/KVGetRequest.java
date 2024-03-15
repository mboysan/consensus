package com.mboysan.consensus.message;

public class KVGetRequest extends Message {
    private final String key;

    public KVGetRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "KVGetRequest{" +
                "key='" + key + '\'' +
                "} " + super.toString();
    }
}
