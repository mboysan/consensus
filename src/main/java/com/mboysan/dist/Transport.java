package com.mboysan.dist;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface Transport {
    void addServer(int nodeId, ProtocolRPC requestProcessor);
    Future<Message> sendRecvAsync(Message message);
    void sendForEach(int senderId, Consumer<ProtocolRPC> protoClient);
    void send(int senderId, int receiverId, Consumer<ProtocolRPC> protoClient);
    int getServerCount();
}
