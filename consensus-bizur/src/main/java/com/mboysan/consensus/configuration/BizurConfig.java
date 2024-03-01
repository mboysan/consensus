package com.mboysan.consensus.configuration;

public interface BizurConfig extends NodeConfig {

    interface Param {
        String NUM_PEERS = "bizur.numPeers";
        String NUM_BUCKETS = "bizur.numBuckets";
        String UPDATE_INTERVAL_MS = "bizur.updateIntervalMs";
    }

    @Key(Param.NUM_PEERS)
    @DefaultValue("0")
    int numPeers();

    @Key(Param.NUM_BUCKETS)
    @DefaultValue("0")
    int numBuckets();

    @Key(Param.UPDATE_INTERVAL_MS)
    @DefaultValue("500")
    long updateIntervalMs();
}
