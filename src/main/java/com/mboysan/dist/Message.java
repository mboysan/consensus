package com.mboysan.dist;

import java.io.Serializable;
import java.util.UUID;

public abstract class Message implements Serializable {
    private String correlationId = UUID.randomUUID().toString();
    private int senderId;
    private int receiverId;

    public <T extends Message> T setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return (T) this;
    }

    public <T extends Message> T setSenderId(int senderId) {
        this.senderId = senderId;
        return (T) this;
    }

    public <T extends Message> T setReceiverId(int receiverId) {
        this.receiverId = receiverId;
        return (T) this;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }

    @Override
    public String toString() {
        return "Message{" +
                "correlationId='" + correlationId + '\'' +
                ", senderId=" + senderId +
                ", receiverId=" + receiverId +
                '}';
    }

    public <REQ extends Message, RESP extends Message> RESP responseTo(REQ request) {
        return this.setCorrelationId(request.getCorrelationId())
                .setSenderId(request.getReceiverId())
                .setReceiverId(request.getSenderId());
    }
}
