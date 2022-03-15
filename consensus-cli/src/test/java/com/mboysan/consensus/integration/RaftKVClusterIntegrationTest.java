package com.mboysan.consensus.integration;

import com.mboysan.consensus.RaftKVStoreCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class RaftKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private RaftKVStoreCluster raftCluster;

    @AfterEach
    void teardown() {
        raftCluster.cleanup();
    }

    @Test
    void testKVOperationsSimple() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(5)
                .build();
        testKVOperationsSimple(raftCluster);
    }

    @Test
    void testKVOperationsMultiThreaded() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(5)
                .build();
        testKVOperationsMultiThreaded(raftCluster);
    }

    @Test
    void testKVStoreShutdownAndStart() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(5)
                .build();
        testKVStoreShutdownAndStart(raftCluster);
    }
}
