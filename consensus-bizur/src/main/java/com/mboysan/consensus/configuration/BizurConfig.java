package com.mboysan.consensus.configuration;

public interface BizurConfig extends NodeConfig {
    @Key("bizur.numPeers")
    @DefaultValue("0")
    int numPeers();

    @Key("bizur.numBuckets")
    @DefaultValue("1")
    int numBuckets();

    @Key("bizur.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();
}
