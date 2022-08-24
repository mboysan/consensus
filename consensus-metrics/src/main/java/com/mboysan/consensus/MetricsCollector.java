package com.mboysan.consensus;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.util.SerializationUtil;
import com.mboysan.consensus.util.ThrowingRunnable;
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

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetricsCollector implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    private static MetricsCollector instance = null;

    private GraphiteFileSender graphiteSender;
    private JvmGcMetrics jvmGcMetrics;  // AutoClosable

    private MetricsAggregator metricsAggregator = null;

    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2);

    private MetricsCollector(MetricsConfig metricsConfig) {
        LOGGER.info("metrics config={}", metricsConfig);
        if (metricsConfig == null) {
            return;
        }

        boolean createGraphiteSender = metricsConfig.insightsMetricsEnabled() || metricsConfig.jvmMetricsEnabled();

        if (createGraphiteSender) {
            String separator = "".equals(metricsConfig.seperator()) ? " " : metricsConfig.seperator();
            graphiteSender = new GraphiteFileSender(metricsConfig.exportfile(), separator);
            LOGGER.debug("created graphite sender for metrics collection.");
        }

        if (metricsConfig.insightsMetricsEnabled()) {
            this.metricsAggregator = new MetricsAggregator(metricsConfig, graphiteSender);
        }

        if (!metricsConfig.jvmMetricsEnabled()) {
            LOGGER.debug("jvm metrics collection disabled.");
            return;
        }

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

        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                .withClock(new DropwizardClock(clock))
                .prefixedWith(null)
                .convertRatesTo(config.rateUnits())
                .convertDurationsTo(config.durationUnits())
                .addMetricAttributesAsTags(config.graphiteTagsEnabled())
                .build(graphiteSender);

        MeterRegistry registry = new GraphiteMeterRegistry(
                config, clock, HierarchicalNameMapper.DEFAULT, metricRegistry, graphiteReporter);

        if (metricsConfig.jvmClassLoaderMetricsEnabled()) {
            new ClassLoaderMetrics().bindTo(registry);
        }
        if (metricsConfig.jvmMemoryMetricsEnabled()) {
            new JvmMemoryMetrics().bindTo(registry);
        }
        if (metricsConfig.jvmGcMetricsEnabled()) {
            jvmGcMetrics = new JvmGcMetrics();
            jvmGcMetrics.bindTo(registry);
        }
        if (metricsConfig.jvmProcessorMetricsEnabled()) {
            new ProcessorMetrics().bindTo(registry);
        }
        if (metricsConfig.jvmThreadMetricsEnabled()) {
            new JvmThreadMetrics().bindTo(registry);
        }
    }

    @Override
    public void close() {
        shutdown(executor::shutdown);
        shutdown(() -> executor.awaitTermination(5000, TimeUnit.MILLISECONDS));
        shutdown(() -> {if (metricsAggregator != null) metricsAggregator.shutdown();});
        shutdown(() -> {if (graphiteSender != null) graphiteSender.shutdown();});
        shutdown(() -> {if (jvmGcMetrics != null) jvmGcMetrics.close();});
    }

    public synchronized static MetricsCollector initAndStart(MetricsConfig metricsConfig) {
        if (instance != null) {
            return instance;
        }
        return instance = new MetricsCollector(metricsConfig);
    }

    public synchronized static MetricsCollector getInstance() {
        if (instance == null) {
            return initAndStart(null);
        }
        return instance;
    }

    /**
     * For testing purposes.
     */
    synchronized static void shutdown() {
        getInstance().close();
        instance = null;    // dereference
    }

    MetricsAggregator getAggregator() {
        return metricsAggregator;
    }

    // ---------------------------------------------------------------------------------- public api

    public boolean isInsightsEnabled() {
        return metricsAggregator != null;
    }

    public void aggregateAsync(String name, long value) {
        executor.submit(() -> {
            aggregate(name, value);
        });
    }

    public void aggregate(String name, long value) {
        if (isInsightsEnabled()) {
            metricsAggregator.add(new LongAggregateMeasurement(name, value));
        }
    }

    public void sampleAsync(String measurementName, Serializable object) {
        executor.submit(() -> {
            sample(measurementName, object);
        });
    }

    public void sample(String measurementName, Serializable object) {
        if (isInsightsEnabled()) {
            try {
                final long objSize = SerializationUtil.sizeOf(object);
                sample(measurementName, objSize);
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    public void sampleAsync(String name, long value) {
        executor.submit(() -> {
            sample(name, value);
        });
    }

    public void sample(String name, long value) {
        sample(name, String.valueOf(value));
    }

    public void sampleAsync(String name, String value) {
        executor.submit(() -> {
            sample(name, value);
        });
    }

    public void sample(String name, String value) {
        if (isInsightsEnabled()) {
            metricsAggregator.add(new Measurement(name, value));
        }
    }

    private static void shutdown(ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
