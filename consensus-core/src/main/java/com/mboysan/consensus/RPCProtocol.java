package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

@FunctionalInterface
interface RPCProtocol extends Function<Message, Message> {
    Message processRequest(Message request) throws IOException;

    @Override
    default Message apply(Message message) {
        try {
            return processRequest(message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
