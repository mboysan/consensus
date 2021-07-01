package com.mboysan.dist;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface RPCProtocol extends Function<Message, Message> {
    RPCProtocol getRPC(Transport transport);
    void onServerListChanged(Set<Integer> serverIds);
    Future<Void> start() throws IOException;
    void shutdown();
}
