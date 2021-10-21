package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;

import java.util.Properties;

public interface NodeInternals<N extends AbstractNode<?>> {

    Class<N> getNodeType();

    default N createNode(int nodeId, Transport transport, Timers timer) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        return createNode(properties, transport, timer);
    }

    N createNode(Properties properties, Transport transport, Timers timer);

    long getElectionTimeoutOf(N node);
    int getLeaderIdOf(N node);
}
