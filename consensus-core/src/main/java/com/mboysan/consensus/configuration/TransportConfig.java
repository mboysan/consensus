package com.mboysan.consensus.configuration;

public interface TransportConfig extends CoreConfig {
    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("5000")
    long messageCallbackTimeoutMs();
}
