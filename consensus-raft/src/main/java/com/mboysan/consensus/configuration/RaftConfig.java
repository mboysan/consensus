package com.mboysan.consensus.configuration;

public interface RaftConfig extends NodeConfig {

    interface Param {
        String UPDATE_INTERVAL_MS = "raft.updateIntervalMs";
        String ELECTION_TIMEOUT_MS = "raft.electionTimeoutMs";
        String CONSISTENCY = "raft.consistency";
    }

    @Key(Param.UPDATE_INTERVAL_MS)
    @DefaultValue("500")
    long updateIntervalMs();

    @Key(Param.ELECTION_TIMEOUT_MS)
    @DefaultValue("1000")
    long electionTimeoutMs();

    @Key(Param.CONSISTENCY)
    @DefaultValue("strong" /* or weak */)
    String consistency();
}
