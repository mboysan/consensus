package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

public interface Transport {
    void registerMessageProcessor(UnaryOperator<Message> messageProcessor);

    Set<Integer> getDestinationNodeIds();

    Future<Message> sendRecvAsync(Message message) throws IOException;

    Message sendRecv(Message message) throws IOException;

    void start() throws IOException;

    void shutdown();

    boolean isShared();
}
