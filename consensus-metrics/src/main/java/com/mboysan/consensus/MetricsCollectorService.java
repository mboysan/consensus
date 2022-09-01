package com.mboysan.consensus;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.util.SerializationUtil;
import com.mboysan.consensus.util.ShutdownUtil;
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

public final class MetricsCollectorService implements BackgroundService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollectorService.class);

    private static MetricsCollectorService instance = null;

    private GraphiteFileSender graphiteSender;
    private JvmGcMetrics jvmGcMetrics;  // AutoClosable

    private MetricsAggregator metricsAggregator = null;

    private MetricsCollectorService(MetricsConfig metricsConfig) {
        BackgroundServiceRegistry.getInstance().register(this);

        LOGGER.info("metrics config={}", metricsConfig);
        if (metricsConfig == null) {
            return;
        }

        boolean createGraphiteSender = metricsConfig.insightsMetricsEnabled() || metricsConfig.jvmMetricsEnabled();

        if (createGraphiteSender) {
            String separator = "".equals(metricsConfig.separator()) ? " " : metricsConfig.separator();
            graphiteSender = new GraphiteFileSender(metricsConfig.exportfile(), separator);
            LOGGER.debug("created graphite sender for metrics collection.");
        }

        if (metricsConfig.insightsMetricsEnabled()) {
            this.metricsAggregator = new MetricsAggregator(metricsConfig, graphiteSender);
            EventManagerService.getInstance().register(MeasurementEvent.class, this::measure);
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

    private void measure(MeasurementEvent measurementEvent) {
        String value = null;
        Object payload = measurementEvent.getPayload();
        if (payload instanceof Long val) {
            value = String.valueOf(val);
        } else if (payload instanceof Integer val) {
            value = String.valueOf(val);
        } else if (payload instanceof String val) {
            value = val;
        } else if (payload instanceof Serializable val) {
            // WARN! other primitives will also use this path
            try {
                value = String.valueOf(SerializationUtil.sizeOf(val));
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        } else {
            throw new UnsupportedOperationException("unsupported measurement payload=" + payload);
        }

        switch (measurementEvent.getMeasurementType()) {
            case SAMPLE ->
                    metricsAggregator.add(new Measurement(
                        measurementEvent.getName(),
                        value,
                        measurementEvent.getTimestamp()
                    ));
            case AGGREGATE ->
                    metricsAggregator.add(new LongAggregateMeasurement(
                        measurementEvent.getName(),
                        String.valueOf(value),
                        measurementEvent.getTimestamp()
                    ));
        }
    }

    public synchronized void shutdown() {
        ShutdownUtil.shutdown(LOGGER, () -> {if (metricsAggregator != null) metricsAggregator.shutdown();});
        ShutdownUtil.shutdown(LOGGER, () -> {if (graphiteSender != null) graphiteSender.shutdown();});
        ShutdownUtil.shutdown(LOGGER, () -> {if (jvmGcMetrics != null) jvmGcMetrics.close();});
    }

    @Override
    public String toString() {
        return "MetricsCollectorService";
    }

    public static synchronized MetricsCollectorService initAndStart(MetricsConfig metricsConfig) {
        if (instance != null) {
            return instance;
        }
        instance = new MetricsCollectorService(metricsConfig);
        return instance;
    }

    /**
     * For testing purposes.
     */
    static void shutdownAndDereference() {
        instance.shutdown();
        instance = null;    // dereference
    }

}
