package com.mboysan.consensus;

public class KVOperationException extends Exception {
    public KVOperationException(String message) {
        super(message);
    }

    public KVOperationException(Throwable cause) {
        super(cause);
    }
}
