package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;

import java.util.Properties;

public interface RaftInternals extends NodeInternals<RaftNode> {
    @Override
    default Class<RaftNode> getNodeType() {
        return RaftNode.class;
    }

    @Override
    default RaftNode createNode(Properties properties, Transport transport, Timers timer) {
        return new RaftNode(IConfig.newInstance(RaftConfig.class, properties), transport) {
            @Override
            Timers createTimers() {
                return timer;
            }
        };
    }

    @Override
    default int getLeaderIdOf(RaftNode node) {
        return node.getState().leaderId;
    }

    @Override
    default long getElectionTimeoutOf(RaftNode node) {
        return ((RaftConfig) node.getNodeConfig()).electionTimeoutMs();
    }
}
