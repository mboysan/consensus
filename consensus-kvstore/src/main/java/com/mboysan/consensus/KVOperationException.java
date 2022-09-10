package com.mboysan.consensus;

import com.mboysan.consensus.message.CommandException;

public class KVOperationException extends CommandException {
    public KVOperationException(Throwable cause) {
        super(cause);
    }
}
