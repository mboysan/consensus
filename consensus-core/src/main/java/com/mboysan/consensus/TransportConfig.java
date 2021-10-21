package com.mboysan.consensus;

public interface TransportConfig extends IConfig {
    @Key("transport.class.name")
        // NettyTransport can be used
    String transportClassName();

    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("5000")
    long messageCallbackTimeoutMs();
}
