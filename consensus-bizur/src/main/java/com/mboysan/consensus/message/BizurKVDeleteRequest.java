package com.mboysan.consensus.message;

public class BizurKVDeleteRequest extends Message {
    private final String key;

    public BizurKVDeleteRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "BizurKVDeleteRequest{" +
                "key='" + key + '\'' +
                "} " + super.toString();
    }
}
