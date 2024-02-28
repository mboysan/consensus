package com.mboysan.consensus.configuration;

public interface InVMTransportConfig extends CoreConfig {

    interface Param {
        String MESSAGE_CALLBACK_TIMEOUT_MS = "transport.message.callbackTimeoutMs";
    }

    @Key(Param.MESSAGE_CALLBACK_TIMEOUT_MS)
    @DefaultValue("250")
    long messageCallbackTimeoutMs();
}
