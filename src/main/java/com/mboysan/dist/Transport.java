package com.mboysan.dist;

import java.util.concurrent.Future;

public interface Transport extends AutoCloseable {
    void addServer(int nodeId, RPCProtocol requestProcessor);
    void removeServer(int nodeId);
    Future<Message> sendRecvAsync(Message message);
    Message sendRecv(Message message);
}
