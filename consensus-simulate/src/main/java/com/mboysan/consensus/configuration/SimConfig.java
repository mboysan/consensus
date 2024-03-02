package com.mboysan.consensus.configuration;

public interface SimConfig extends NodeConfig {

    interface Param {
        String LEADER_ID = "simulate.leaderId";
        String FORWARD_TO_LEADER = "simulate.follower.forwardToLeader";
        String BROADCAST_TO_FOLLOWERS = "simulate.leader.broadcastToFollowers";
    }

    @Key(Param.LEADER_ID)
    @DefaultValue("0")
    int leaderId();

    @Key(Param.FORWARD_TO_LEADER)
    @DefaultValue("true")
    boolean forwardToLeader();

    @Key(Param.BROADCAST_TO_FOLLOWERS)
    @DefaultValue("true")
    boolean broadcastToFollowers();

}
