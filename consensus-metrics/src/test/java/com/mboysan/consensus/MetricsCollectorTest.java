package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.util.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    @Test
    void testMetricsCollectionDisabled() throws IOException {
        Properties properties = new Properties();
        properties.put("metrics.enabled", "false");
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
    void testMetricsCollectionEnabled() throws IOException {
        Properties properties = new Properties();
        properties.put("metrics.enabled", "true");
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
    void testMetricsCollectionWithPrefix() throws IOException, InterruptedException {
        String expectedPrefix = "part1.part2";
        Properties properties = new Properties();
        properties.put("metrics.enabled", "true");
        properties.put("metrics.step", "1000");
        properties.put("metrics.prefix", expectedPrefix);
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
                assertTrue(metric.startsWith(expectedPrefix));
            }
        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

    @Test
    void testMetricsCollectionWithSeparator() throws IOException, InterruptedException {
        String separator = "###";
        Properties properties = new Properties();
        properties.put("metrics.enabled", "true");
        properties.put("metrics.step", "1000");
        properties.put("metrics.seperator", separator);
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

}