package com.mboysan.consensus.integration;

import com.mboysan.consensus.CliConstants;
import com.mboysan.consensus.SimKVStoreCluster;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    }

    @Test
    void testCustomCommands() throws Exception {
        this.simCluster = new SimKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();

        String response;

        response = simCluster.getClient(0).customRequest("askState");
        assertTrue(response.startsWith("Verbose State of node"));

        response = simCluster.getClient(0).customRequest("askState", 1);
        assertTrue(response.startsWith("Verbose State of node-1"));

        response = simCluster.getClient(0).customRequest("askStateFull");
        assertTrue(response.startsWith("Verbose State of node"));

        response = simCluster.getClient(0).customRequest("askStateFull", 1);
        assertTrue(response.startsWith("Verbose State of node-1"));

        response = simCluster.getClient(0).customRequest("askProtocol");
        assertEquals(CliConstants.Protocol.SIMULATE, response);
    }

}
