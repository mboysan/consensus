package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
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
    void assertSamplingAndAggregation() throws IOException {
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

            collector.sample("sampledStr", "value0");
            collector.sampleAsync("sampledStr", "value1");
            collector.sample("sampledStr", "value2");

            collector.sample("sampledLong", 0);
            collector.sampleAsync("sampledLong", 1);
            collector.sample("sampledLong", 2);

            collector.sample("sampledMessageSize", new CustomRequest(""));
            collector.sampleAsync("sampledMessageSize", new CustomRequest(""));
            collector.sample("sampledMessageSize", new CustomRequest(""));

            collector.aggregate("aggregatedLong", 0);
            collector.aggregateAsync("aggregatedLong", 10);
            collector.aggregate("aggregatedLong", 20);

            collector.close();   // measurements will be dumped upon close.

            AtomicInteger sampledStrCount = new AtomicInteger(0);
            AtomicInteger sampledLongCount = new AtomicInteger(0);
            AtomicLong sampledMessageSizes = new AtomicLong(0);
            AtomicLong aggregatedLong = new AtomicLong(0);

            List<String> metricsLines = Files.readAllLines(metricsPath);
            for (String metric : metricsLines) {
                String[] split = metric.split(separator);
                final String name = split[0];
                final String value = split[1];
                final String timestamp = split[2];

                assertNotNull(timestamp);

                if (name.equals("sampledStr")) {
                    sampledStrCount.incrementAndGet();
                }
                if (name.equals("sampledLong")) {
                    sampledLongCount.incrementAndGet();
                }
                if (name.equals("sampledMessageSize")) {
                    sampledMessageSizes.set(sampledMessageSizes.get() + Long.parseLong(value));
                }
                if (name.equals("aggregatedLong")) {
                    aggregatedLong.set(Long.parseLong(value));
                }
            }

            assertEquals(3, sampledStrCount.get());
            assertEquals(3, sampledLongCount.get());
            assertTrue(sampledMessageSizes.get() > 600);
            assertEquals(30, aggregatedLong.get());

        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

}