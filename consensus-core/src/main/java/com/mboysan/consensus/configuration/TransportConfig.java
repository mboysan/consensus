package com.mboysan.consensus.configuration;

public interface TransportConfig extends CoreConfig {
    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("1000")
    long messageCallbackTimeoutMs();
}
