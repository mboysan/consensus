package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import org.aeonbits.owner.ConfigFactory;

import java.util.Properties;

public interface BizurInternals extends NodeInternals<BizurNode> {

    @Override
    default Class<BizurNode> getNodeType() {
        return BizurNode.class;
    }

    @Override
    default BizurNode createNode(Properties properties, Transport transport, Timers timer) {
        return new BizurNode(ConfigFactory.create(BizurConfig.class, properties), transport) {
            @Override
            Timers createTimers() {
                return timer;
            }
        };
    }

    @Override
    default long getElectionTimeoutOf(BizurNode node) {
        return ((BizurConfig) node.getNodeConfig()).electionTimeoutMs();
    }

    @Override
    default int getLeaderIdOf(BizurNode node) {
        return node.getBizurStateUnprotected().getLeaderId();
    }

    @Override
    default KVStore createKVStore(BizurNode node) {
        return new BizurKVStore(node);
    }
}
