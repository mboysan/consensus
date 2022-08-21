package com.mboysan.consensus.integration;

import com.mboysan.consensus.SimKVStoreCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private SimKVStoreCluster simCluster;

    @AfterEach
    void teardown() {
        simCluster.cleanup();
    }

    @Test
    void testAllOperations() throws Exception {
        this.simCluster = new SimKVStoreCluster.Builder()
                .setNumNodes(5)
                .build();

        // just check that no exceptions are being thrown.

        simCluster.getRandomClient().set("a", "b");
        simCluster.getRandomClient().get("a");
        simCluster.getRandomClient().delete("a");
        simCluster.getRandomClient().iterateKeys();
    }

    @Test
    void testCustomCommands() throws Exception {
        this.simCluster = new SimKVStoreCluster.Builder()
                .setNumNodes(5)
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
        assertEquals("simulate", response);
    }

}
