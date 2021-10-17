package com.mboysan.consensus;

import com.mboysan.consensus.util.TimersForTesting;

public interface NodeInternals<N extends AbstractNode<?>> {
    Class<N> getNodeType();
    N createNode(int nodeId, Transport transport, TimersForTesting timer);
    long getElectionTimeoutMsOf(N node);
    int getLeaderIdOf(N node);
    KVStore createKVStore(N node);
}
