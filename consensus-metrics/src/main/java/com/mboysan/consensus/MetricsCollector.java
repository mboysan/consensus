package com.mboysan.consensus;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mboysan.consensus.configuration.MetricsConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.dropwizard.DropwizardClock;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.graphite.GraphiteConfig;
import io.micrometer.graphite.GraphiteMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class MetricsCollector implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private GraphiteFileSender graphiteSender;
    private JvmGcMetrics jvmGcMetrics;  // AutoClosable

    public MetricsCollector(MetricsConfig metricsConfig) {
        if (!metricsConfig.enabled()) {
            LOGGER.info("metrics collection disabled.");
            return;
        }
        LOGGER.info("metrics collection enabled with config: {}", metricsConfig);

        GraphiteConfig config = new GraphiteConfig() {
            @Override
            public String get(String s) {
                return null;
            }
            @Override
            public Duration step() {
                return Duration.ofMillis(metricsConfig.step());
            }
        };

        MetricRegistry metricRegistry = new MetricRegistry();
        Clock clock = Clock.SYSTEM;

        String prefix = "".equals(metricsConfig.prefix()) ? null : metricsConfig.prefix();
        graphiteSender = new GraphiteFileSender(metricsConfig.outputPath());
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                .withClock(new DropwizardClock(clock))
                .prefixedWith(prefix)
                .convertRatesTo(config.rateUnits())
                .convertDurationsTo(config.durationUnits())
                .addMetricAttributesAsTags(config.graphiteTagsEnabled())
                .build(graphiteSender);

        MeterRegistry registry = new GraphiteMeterRegistry(
                config, clock, HierarchicalNameMapper.DEFAULT, metricRegistry, graphiteReporter);

        if (metricsConfig.classLoaderMetricsEnabled()) {
            new ClassLoaderMetrics().bindTo(registry);
        }
        if (metricsConfig.memoryMetricsEnabled()) {
            new JvmMemoryMetrics().bindTo(registry);
        }
        if (metricsConfig.gcMetricsEnabled()) {
            jvmGcMetrics = new JvmGcMetrics();
            jvmGcMetrics.bindTo(registry);
        }
        if (metricsConfig.processorMetricsEnabled()) {
            new ProcessorMetrics().bindTo(registry);
        }
        if (metricsConfig.threadMetricsEnabled()) {
            new JvmThreadMetrics().bindTo(registry);
        }
    }

    @Override
    public void close() {
        if (graphiteSender != null) {
            graphiteSender.shutdown();
        }
        if (jvmGcMetrics != null) {
            jvmGcMetrics.close();
        }
    }
}
