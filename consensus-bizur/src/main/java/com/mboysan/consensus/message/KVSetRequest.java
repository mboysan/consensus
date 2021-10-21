package com.mboysan.consensus.message;

public class KVSetRequest extends Message {
    private final String key;
    private final String value;

    public KVSetRequest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KVSetRequest{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                "} " + super.toString();
    }
}
