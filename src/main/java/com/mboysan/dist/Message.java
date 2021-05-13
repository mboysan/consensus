package com.mboysan.dist;

import java.io.Serializable;

public class Message implements Serializable {
    final String correlationId;
    final int senderId;
    final int receiverId;
    final Serializable command;
    final int retryCount;

    public Message(String correlationId, int senderId, int receiverId, Serializable command) {
        this(correlationId, senderId, receiverId, command, 0);
    }

    public Message(String correlationId, int senderId, int receiverId, Serializable command, int retryCount) {
        this.correlationId = correlationId;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.command = command;
        this.retryCount = retryCount;
    }

    public Message(Message other) {
        this(
                other.getCorrelationId(),
                other.getSenderId(),
                other.getReceiverId(),
                other.getCommand(),
                other.getRetryCount() + 1
        );

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

    public Serializable getCommand() {
        return command;
    }

    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public String toString() {
        return "Message{" +
                "correlationId='" + correlationId + '\'' +
                ", senderId=" + senderId +
                ", receiverId=" + receiverId +
                ", command=" + command +
                ", retryCount=" + retryCount +
                '}';
    }
}
