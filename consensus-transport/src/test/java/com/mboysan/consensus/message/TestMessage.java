package com.mboysan.consensus.message;

public class TestMessage extends Message {
    private final String payload;

    public TestMessage(String payload) {
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "TestMessage{" +
                "payload='" + payload + '\'' +
                "} " + super.toString();
    }
}
