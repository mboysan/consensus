package com.mboysan.consensus.configuration;

public interface NodeConfig extends CoreConfig {
    @Key("node.id")
    int nodeId();

    @Key("node.consensus.protocol")
    String nodeConsensusProtocol();

    @Key("node.peerExecutor.threadCount")
    @DefaultValue("0")
    int nodePeerExecutorThreadCount();
}
