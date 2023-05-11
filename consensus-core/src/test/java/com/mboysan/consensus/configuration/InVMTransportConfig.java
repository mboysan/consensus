package com.mboysan.consensus.configuration;

public interface InVMTransportConfig extends CoreConfig {
    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("250")
    long messageCallbackTimeoutMs();
}
