package com.mboysan.consensus.configuration;

public interface RaftConfig extends NodeConfig {
    @Key("raft.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();

    @Key("raft.electionTimeoutMs")
    @DefaultValue("1000")
    long electionTimeoutMs();

    @Key("raft.consistency")
    @DefaultValue("strong") // or weak
    String consistency();
}
