package com.mboysan.consensus.integration;

import com.mboysan.consensus.SimKVStoreCluster;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SimKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private SimKVStoreCluster simCluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @AfterEach
    void teardown() {
        simCluster.cleanup();
    }

    @Test
    void testAllOperations() throws Exception {
        this.simCluster = new SimKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();

        assertDoesNotThrow(() -> simCluster.getRandomClient().set("a", "b"));
        assertDoesNotThrow(() -> simCluster.getRandomClient().get("a"));
        assertDoesNotThrow(() -> simCluster.getRandomClient().delete("a"));
        assertDoesNotThrow(() -> simCluster.getRandomClient().iterateKeys());
        assertDoesNotThrow(() -> simCluster.getRandomClient().checkIntegrity(1, -1));
        assertDoesNotThrow(() -> simCluster.getRandomClient().checkIntegrity(1, 1));
    }

    @Test
    void testCustomCommands() throws Exception {
        this.simCluster = new SimKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testCustomCommands(simCluster);
    }

}
