package com.mboysan.consensus.configuration;

public interface NodeConfig extends CoreConfig {

    interface Param {
        String NODE_ID = "node.id";
        String NODE_CONSENSUS_PROTOCOL = "node.consensus.protocol";
        String NODE_PEER_EXECUTOR_THREAD_COUNT = "node.peerExecutor.threadCount";
    }

    @Key(Param.NODE_ID)
    int nodeId();

    @Key(Param.NODE_CONSENSUS_PROTOCOL)
    String nodeConsensusProtocol();

    @Key(Param.NODE_PEER_EXECUTOR_THREAD_COUNT)
    @DefaultValue("0")
    int nodePeerExecutorThreadCount();
}
