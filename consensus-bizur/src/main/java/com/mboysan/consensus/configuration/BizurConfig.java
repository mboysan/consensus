package com.mboysan.consensus.configuration;

public interface BizurConfig extends Configuration {
    @Key("bizur.numBuckets")
    @DefaultValue("1")
    int numBuckets();

    @Key("bizur.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();
}
