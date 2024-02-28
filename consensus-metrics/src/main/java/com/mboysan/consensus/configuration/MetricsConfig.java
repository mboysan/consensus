package com.mboysan.consensus.configuration;

public interface MetricsConfig extends CoreConfig {

    interface Param {
        String EXPORTFILE = "metrics.exportfile";
        String SEPARATOR = "metrics.separator";
        String STEP = "metrics.step";
        String INSIGHTS_ENABLED = "metrics.insights.enabled";
        String JVM_ENABLED = "metrics.jvm.enabled";
        String JVM_CLASSLOADER_ENABLED = "metrics.jvm.classloader.enabled";
        String JVM_MEMORY_ENABLED = "metrics.jvm.memory.enabled";
        String JVM_GC_ENABLED = "metrics.jvm.gc.enabled";
        String JVM_PROCESSOR_ENABLED = "metrics.jvm.processor.enabled";
        String JVM_THREAD_ENABLED = "metrics.jvm.thread.enabled";
    }

    @Key(Param.EXPORTFILE)
    @DefaultValue("/tmp/metrics.txt")
    String exportfile();

    @Key(Param.SEPARATOR)
    @DefaultValue(" ")
    String separator();

    @Key(Param.STEP)
    @DefaultValue("2000")
    long step();

    @Key(Param.INSIGHTS_ENABLED)
    @DefaultValue("false")
    boolean insightsMetricsEnabled();

    @Key(Param.JVM_ENABLED)
    @DefaultValue("false")
    boolean jvmMetricsEnabled();

    @Key(Param.JVM_CLASSLOADER_ENABLED)
    @DefaultValue("true")
    boolean jvmClassLoaderMetricsEnabled();

    @Key(Param.JVM_MEMORY_ENABLED)
    @DefaultValue("true")
    boolean jvmMemoryMetricsEnabled();

    @Key(Param.JVM_GC_ENABLED)
    @DefaultValue("true")
    boolean jvmGcMetricsEnabled();

    @Key(Param.JVM_PROCESSOR_ENABLED)
    @DefaultValue("true")
    boolean jvmProcessorMetricsEnabled();

    @Key(Param.JVM_THREAD_ENABLED)
    @DefaultValue("true")
    boolean jvmThreadMetricsEnabled();

}
