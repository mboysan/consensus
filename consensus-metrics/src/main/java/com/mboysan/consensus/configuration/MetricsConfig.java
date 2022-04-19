package com.mboysan.consensus.configuration;

public interface MetricsConfig extends CoreConfig {

    @Key("metrics.enabled")
    @DefaultValue("false")
    boolean enabled();

    @Key("metrics.output.path")
    @DefaultValue("/tmp/metrics.txt")
    String outputPath();

    @Key("metrics.prefix")
    @DefaultValue("")
    String prefix();

    @Key("metrics.step")
    @DefaultValue("2000")
    long step();

    @Key("metrics.classloader.enabled")
    @DefaultValue("true")
    boolean classLoaderMetricsEnabled();

    @Key("metrics.memory.enabled")
    @DefaultValue("true")
    boolean memoryMetricsEnabled();

    @Key("metrics.gc.enabled")
    @DefaultValue("true")
    boolean gcMetricsEnabled();

    @Key("metrics.processor.enabled")
    @DefaultValue("true")
    boolean processorMetricsEnabled();

    @Key("metrics.thread.enabled")
    @DefaultValue("true")
    boolean threadMetricsEnabled();

}
