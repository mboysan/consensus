package com.mboysan.consensus.configuration;

public interface MetricsConfig extends CoreConfig {

    @Key("metrics.exportfile")
    @DefaultValue("/tmp/metrics.txt")
    String exportfile();

    @Key("metrics.separator")
    @DefaultValue(" ")
    String seperator();

    @Key("metrics.step")
    @DefaultValue("2000")
    long step();

    @Key("metrics.insights.enabled")
    @DefaultValue("false")
    boolean insightsMetricsEnabled();

    @Key("metrics.jvm.enabled")
    @DefaultValue("false")
    boolean jvmMetricsEnabled();

    @Key("metrics.jvm.classloader.enabled")
    @DefaultValue("true")
    boolean jvmClassLoaderMetricsEnabled();

    @Key("metrics.jvm.memory.enabled")
    @DefaultValue("true")
    boolean jvmMemoryMetricsEnabled();

    @Key("metrics.jvm.gc.enabled")
    @DefaultValue("true")
    boolean jvmGcMetricsEnabled();

    @Key("metrics.jvm.processor.enabled")
    @DefaultValue("true")
    boolean jvmProcessorMetricsEnabled();

    @Key("metrics.jvm.thread.enabled")
    @DefaultValue("true")
    boolean jvmThreadMetricsEnabled();

}
