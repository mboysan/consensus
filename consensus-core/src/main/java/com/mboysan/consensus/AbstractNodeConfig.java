package com.mboysan.consensus;

public interface AbstractNodeConfig extends CoreConfig {
    @Key("node.id")
    int nodeId();
}