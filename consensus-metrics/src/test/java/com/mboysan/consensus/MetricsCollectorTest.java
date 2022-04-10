package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.util.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    @Test
    void testMetricsCollectionDisabled() throws IOException {
        Properties properties = new Properties();
        properties.put("metrics.enabled", "false");
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        Path metricsPath = FileUtil.path(config.outputPath());
        try {
            MetricsCollector collector = new MetricsCollector(config);
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
        Path metricsPath = FileUtil.path(config.outputPath());
        try {
            MetricsCollector collector = new MetricsCollector(config);
            assertTrue(Files.exists(metricsPath));
            collector.close();
        } finally {
            Files.deleteIfExists(metricsPath);
        }
    }

}