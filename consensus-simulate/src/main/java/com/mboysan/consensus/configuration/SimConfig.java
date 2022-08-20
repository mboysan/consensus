package com.mboysan.consensus.configuration;

public interface SimConfig extends NodeConfig {

    @Key("simulate.follower.forwardToLeader")
    @DefaultValue("true")
    boolean forwardToLeader();

    @Key("simulate.leader.broadcastToFollowers")
    @DefaultValue("true")
    boolean broadcastToFollowers();

}
