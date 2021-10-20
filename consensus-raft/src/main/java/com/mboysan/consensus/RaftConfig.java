package com.mboysan.consensus;

public interface RaftConfig extends AbstractNodeConfig {
    @Key("raft.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();

    @Key("raft.electionTimeoutMs")
    @DefaultValue("5000")
    long electionTimeoutMs();
}
