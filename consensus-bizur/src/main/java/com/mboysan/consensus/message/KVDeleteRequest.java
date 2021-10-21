package com.mboysan.consensus.message;

public class KVDeleteRequest extends Message {
    private final String key;

    public KVDeleteRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "KVDeleteRequest{" +
                "key='" + key + '\'' +
                "} " + super.toString();
    }
}
