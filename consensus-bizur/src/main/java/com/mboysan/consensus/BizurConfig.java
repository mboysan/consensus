package com.mboysan.consensus;

public interface BizurConfig extends AbstractNodeConfig {
    @Key("bizur.numBuckets")
    @DefaultValue("1")
    int numBuckets();

    @Key("bizur.updateIntervalMs")
    @DefaultValue("500")
    long updateIntervalMs();

    @Key("bizur.electionTimeoutMs")
    @DefaultValue("5000")
    long electionTimeoutMs();
}
