package com.mboysan.consensus;

import com.mboysan.consensus.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Future;

public abstract class AbstractKVStore<N extends AbstractNode<?>> implements KVStoreRPC {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKVStore.class);

    private final N node;
    private final Transport clientServingTransport;

    public AbstractKVStore(N node, Transport clientServingTransport) {
        this.node = node;
        this.clientServingTransport = clientServingTransport;
    }

    @Override
    public Future<Void> start() throws IOException {
        clientServingTransport.registerMessageProcessor(this);
        clientServingTransport.start();
        return node.start();
    }

    @Override
    public void shutdown() {
        clientServingTransport.shutdown();
        node.shutdown();
    }

    N getNode() {
        return node;
    }

    void logError(Message request, Exception err) {
        LOGGER.error("on {} KVStore-{}, error={}, for request={}",
                getNode().getClass().getSimpleName(), getNode().getNodeId(), err, request.toString());
    }
}
