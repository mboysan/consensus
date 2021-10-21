package com.mboysan.consensus;

public interface NodeConfig extends IConfig {
    @Key("node.id")
    int nodeId();
}