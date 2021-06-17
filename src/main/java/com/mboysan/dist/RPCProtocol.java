package com.mboysan.dist;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface RPCProtocol extends Function<Message, Message> {
    RPCProtocol getRPC(Transport transport);
    void onServerListChanged(Set<Integer> serverIds);
    Future<Void> start();
    void shutdown();
}
