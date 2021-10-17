package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import com.mboysan.consensus.util.TimersForTesting;

public interface RaftInternals extends NodeInternals<RaftNode> {
    @Override
    default Class<RaftNode> getNodeType() {
        return RaftNode.class;
    }

    @Override
    default RaftNode createNode(int nodeId, Transport transport, TimersForTesting timer) {
        return new RaftNode(nodeId, transport) {
            @Override
            Timers createTimers() {
                return timer;
            }
        };
    }

    @Override
    default long getElectionTimeoutMsOf(RaftNode node) {
        return node.electionTimeoutMs;
    }

    @Override
    default int getLeaderIdOf(RaftNode node) {
        return node.getState().leaderId;
    }

    @Override
    default KVStore createKVStore(RaftNode node) {
        return new RaftKVStore(node);
    }
}
