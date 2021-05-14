package com.mboysan.dist;

import java.util.concurrent.Future;

public interface Transport {
    void addServer(int nodeId, RPCProtocol requestProcessor);
    Future<Message> sendRecvAsync(Message message);
    Message sendRecv(Message message);
}
