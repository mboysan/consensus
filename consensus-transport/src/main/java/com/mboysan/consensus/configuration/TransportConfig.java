package com.mboysan.consensus.configuration;

public interface TransportConfig extends CoreConfig {
    /**
     * Supported object transportation method. Currently supported types are:
     * <l>
     *     <li>invm: All objects are transported between server and clients inside the same jvm using queues.</li>
     *     <li>tcp: All objects are transported between server and clients using TCP sockets.</li>
     * </l>
     */
    @Key("transport.type")
    String type();

    @Key("transport.clientPoolSize")
    @DefaultValue("-1")
    int clientPoolSize();

    @Key("transport.message.callbackTimeoutMs")
    @DefaultValue("5000")
    long messageCallbackTimeoutMs();

    @Key("transport.failureDetector.markServerAsFailedCount")
    @DefaultValue("3")
    int markServerAsFailedCount();

    @Key("transport.failureDetector.pingInterval")
    @DefaultValue("5000")   // 5 seconds
    long pingInterval();
}
