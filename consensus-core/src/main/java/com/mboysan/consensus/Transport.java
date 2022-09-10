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

    /**
     * Determines whether this transport instance is shared with other nodes. An appropriate use-case for this is
     * when running unit tests where the same transport instance can be used to pass messages along the participants.
     * <br>
     * <b>NB!</b> If this transport is shared, the {@link #shutdown()} method must be called explicitly after
     * finishing the usage of this transport.
     * <br>
     * See: InVMTransport test class.
     * @return true if this transport instance is shared with other nodes. false, otherwise.
     */
    default boolean isShared() {
        return false;
    }
}
