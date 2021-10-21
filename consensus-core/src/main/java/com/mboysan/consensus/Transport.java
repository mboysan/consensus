package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;

import java.io.IOException;
import java.util.concurrent.Future;

public interface Transport {
    void addNode(int nodeId, RPCProtocol requestProcessor);

    void removeNode(int nodeId);

    Future<Message> sendRecvAsync(Message message) throws IOException;

    Message sendRecv(Message message) throws IOException;

    void start() throws IOException;

    void shutdown();

    boolean isShared();
}
