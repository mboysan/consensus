package com.mboysan.consensus.message;

public class BizurKVSetRequest extends Message {
    private final String key;
    private final String value;

    public BizurKVSetRequest(String key, String value) {
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
        return "BizurKVSetRequest{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                "} " + super.toString();
    }
}
