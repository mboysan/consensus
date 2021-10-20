package com.mboysan.consensus;

import org.aeonbits.owner.ConfigCache;

import java.util.Properties;

public interface TransportConfig extends CoreConfig {
    @Key("transport.class.name")
        // NettyTransport can be used
    String transportClassName();

    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("5000")
    long messageCallbackTimeoutMs();

    static TransportConfig getCached(Properties... properties) {
        return ConfigCache.getOrCreate(TransportConfig.class, properties);
    }
}
