package com.mboysan.consensus.integration;

import com.mboysan.consensus.SimKVStoreCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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

}
