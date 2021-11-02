package com.mboysan.consensus.configuration;

public interface RaftConfig extends Configuration {
    @Key("raft.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();

    @Key("raft.electionTimeoutMs")
    @DefaultValue("5000")
    long electionTimeoutMs();
}
