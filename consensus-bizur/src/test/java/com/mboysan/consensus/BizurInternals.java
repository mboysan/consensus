package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.util.Timers;

import java.util.Properties;

public interface BizurInternals extends NodeInternals<BizurNode> {

    @Override
    default Class<BizurNode> getNodeType() {
        return BizurNode.class;
    }

    @Override
    default BizurNode createNode(Properties properties, Transport transport, Timers timer) {
        return new BizurNode(Configuration.newInstance(BizurConfig.class, properties), transport) {
            @Override
            Timers createTimers() {
                return timer;
            }
        };
    }

    @Override
    default long getElectionTimeoutOf(BizurNode node) {
        return ((BizurConfig) node.getConfiguration()).updateIntervalMs();
    }

    @Override
    default int getLeaderIdOf(BizurNode node) {
        throw new UnsupportedOperationException("");
    }
}
