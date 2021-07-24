package com.mboysan.consensus;

import java.io.IOException;
import java.util.concurrent.Future;

public interface Transport {
    void addServer(int nodeId, RPCProtocol requestProcessor);
    void removeServer(int nodeId);
    Future<Message> sendRecvAsync(Message message) throws IOException;
    Message sendRecv(Message message) throws IOException;
    void start() throws IOException;
    void shutdown();
    boolean isShared();
}