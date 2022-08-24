package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.event.MeasurementAsyncEvent;
import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.mboysan.consensus.event.MeasurementEvent.MeasurementType.AGGREGATE;
import static com.mboysan.consensus.event.MeasurementEvent.MeasurementType.SAMPLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsCollectorTest {

    @AfterEach
    void tearDown() {
        MetricsCollector.shutdown();
    }

    // --------------------------------------------------------------------------------- jvm metrics

    @Test
    void testJvmMetricsCollectionDisabled() throws IOException {
        Properties properties = new Properties();
        properties.put("metrics.jvm.enabled", "false");
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        Path metricsPath = FileUtil.path(config.exportfile());
        try {
            MetricsCollector collector = MetricsCollector.initAndStart(config);
            assertFalse(Files.exists(metricsPath));
            collector.close();
        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

    @Test
    void testJvmMetricsCollectionEnabled() throws IOException {
        Properties properties = new Properties();
        properties.put("metrics.jvm.enabled", "true");
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        Path metricsPath = FileUtil.path(config.exportfile());
        try {
            MetricsCollector collector = MetricsCollector.initAndStart(config);
            assertTrue(Files.exists(metricsPath));
            collector.close();
        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

    @Test
    void testJvmMetricsCollectionWithSeparator() throws IOException, InterruptedException {
        String separator = "###";
        Properties properties = new Properties();
        properties.put("metrics.jvm.enabled", "true");
        properties.put("metrics.step", "1000");
        properties.put("metrics.separator", separator);
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        Path metricsPath = FileUtil.path(config.exportfile());
        try {
            MetricsCollector collector = MetricsCollector.initAndStart(config);
            Thread.sleep(3000);
            assertTrue(Files.exists(metricsPath));
            List<String> metricsLines = Files.readAllLines(metricsPath);
            collector.close();

            assertTrue(metricsLines.size() > 0);
            for (String metric : metricsLines) {
                assertTrue(metric.contains(separator));
                // 3 column per line -> name, value and timestamp
                assertEquals(3, metric.split(separator).length);
            }
        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

    // --------------------------------------------------------------------------------- insights metrics

    @Test
    void assertSampleAndAggregate() throws IOException {
        String separator = " ";
        Properties properties = new Properties();
        properties.put("metrics.insights.enabled", "true");
        properties.put("metrics.step", "1000");
        properties.put("metrics.separator", separator);
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        Path metricsPath = FileUtil.path(config.exportfile());
        try {
            MetricsCollector collector = MetricsCollector.initAndStart(config);
            assertTrue(Files.exists(metricsPath));

            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledStr", "value0"));
            EventManager.fireEvent(new MeasurementAsyncEvent(SAMPLE, "sampledStr", "value1"));
            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledStr", "value2"));

            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledInt", 10));
            EventManager.fireEvent(new MeasurementAsyncEvent(SAMPLE, "sampledInt", 20));
            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledInt", 30));

            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledLong", 10L));
            EventManager.fireEvent(new MeasurementAsyncEvent(SAMPLE, "sampledLong", 20L));
            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledLong", 30L));

            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledMessageSize", new CustomRequest("")));
            EventManager.fireEvent(new MeasurementAsyncEvent(SAMPLE, "sampledMessageSize", new CustomRequest("")));
            EventManager.fireEvent(new MeasurementEvent(SAMPLE, "sampledMessageSize", new CustomRequest("")));

            EventManager.fireEvent(new MeasurementEvent(AGGREGATE, "aggregatedLong", 10L));
            EventManager.fireEvent(new MeasurementAsyncEvent(AGGREGATE, "aggregatedLong", 20L));
            EventManager.fireEvent(new MeasurementEvent(AGGREGATE, "aggregatedLong", 30L));

            collector.close();   // measurements will be dumped upon close.

            AtomicInteger sampledStrCount = new AtomicInteger(0);
            AtomicInteger sampledIntTotal = new AtomicInteger(0);
            AtomicLong sampledLongTotal = new AtomicLong(0);
            AtomicLong sampledMessageSizes = new AtomicLong(0);
            AtomicLong aggregatedLong = new AtomicLong(0);

            List<String> metricsLines = Files.readAllLines(metricsPath);
            assertEquals(13, metricsLines.size());  // 12 sample + 1 aggregate
            for (String metric : metricsLines) {
                String[] split = metric.split(separator);
                final String name = split[0];
                final String value = split[1];
                final String timestamp = split[2];

                assertNotNull(timestamp);

                if (name.equals("sampledStr")) {
                    sampledStrCount.incrementAndGet();
                }
                if (name.equals("sampledInt")) {
                    sampledIntTotal.addAndGet(Integer.parseInt(value));
                }
                if (name.equals("sampledLong")) {
                    sampledLongTotal.addAndGet(Long.parseLong(value));
                }
                if (name.equals("sampledMessageSize")) {
                    sampledMessageSizes.set(sampledMessageSizes.get() + Long.parseLong(value));
                }
                if (name.equals("aggregatedLong")) {
                    aggregatedLong.set(Long.parseLong(value));
                }
            }

            assertEquals(3, sampledStrCount.get());
            assertEquals(60, sampledIntTotal.get());
            assertEquals(60, sampledLongTotal.get());
            assertTrue(sampledMessageSizes.get() > 600);
            assertEquals(60, aggregatedLong.get());

        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

}