package com.mboysan.consensus.configuration;

public interface SimConfig extends NodeConfig {

    @Key("simulate.leaderId")
    @DefaultValue("0")
    int leaderId();

    @Key("simulate.follower.forwardToLeader")
    @DefaultValue("true")
    boolean forwardToLeader();

    @Key("simulate.leader.broadcastToFollowers")
    @DefaultValue("true")
    boolean broadcastToFollowers();

}
