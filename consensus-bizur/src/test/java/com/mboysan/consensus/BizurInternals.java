package com.mboysan.consensus;

import com.mboysan.consensus.util.Timers;
import com.mboysan.consensus.util.TimersForTesting;

public interface BizurInternals extends NodeInternals<BizurNode> {
    @Override
    default Class<BizurNode> getNodeType() {
        return BizurNode.class;
    }

    @Override
    default BizurNode createNode(int nodeId, Transport transport, TimersForTesting timer) {
        return new BizurNode(nodeId, transport) {
            @Override
            Timers createTimers() {
                return timer;
            }
        };
    }

    @Override
    default long getElectionTimeoutMsOf(BizurNode node) {
        return node.electionTimeoutMs;
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
