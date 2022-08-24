package com.mboysan.consensus.message;

import com.mboysan.consensus.util.RngUtil;

import java.io.Serializable;

public abstract class Message implements Serializable {
    /**
     * Unique id of a request and response message pair.
     * This value can only be modified with {@link #responseTo(Message)} method, i.e. in response to a certain request.
     */
    private String id = generateId();
    /**
     * id of a group of messages related to each other or a certain context.
     */
    private String correlationId = generateId();
    /**
     * id of the sender node.
     */
    private int senderId = -1;
    /**
     * id of the receiver node.
     */
    private int receiverId = -1;

    @SuppressWarnings("unchecked")
    private <T extends Message> T setId(String id) {
        this.id = id;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T setSenderId(int senderId) {
        this.senderId = senderId;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T setReceiverId(int receiverId) {
        this.receiverId = receiverId;
        return (T) this;
    }

    public String getId() {
        return id;
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
                "id='" + id + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", senderId=" + senderId +
                ", receiverId=" + receiverId +
                '}';
    }

    public <Q extends Message, S extends Message> S responseTo(Q request) {
        return this.setId(request.getId())
                .setCorrelationId(request.getCorrelationId())
                .setSenderId(request.getReceiverId())
                .setReceiverId(request.getSenderId());
    }

    public static String generateId() {
        return RngUtil.nextInt(Integer.MAX_VALUE) + "";
    }
}
