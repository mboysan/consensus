package com.mboysan.consensus.integration;

import com.mboysan.consensus.RaftKVStoreCluster;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class RaftKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private RaftKVStoreCluster raftCluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @AfterEach
    void teardown() {
        raftCluster.cleanup();
    }

    @Test
    void testKVOperationsSimple() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testKVOperationsSimple(raftCluster);
    }

    @Test
    void testKVOperationsSequential() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testKVOperationsSequential(raftCluster);
    }

    @Test
    void testKVOperationsMultiThreaded() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testKVOperationsMultiThreaded(raftCluster);
    }

    @Test
    void testKVStoreShutdownAndStart() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testKVStoreShutdownAndStart(raftCluster);
    }

    @Test
    void testCustomCommands() throws Exception {
        this.raftCluster = new RaftKVStoreCluster.Builder()
                .setNumNodes(3)
                .build();
        testCustomCommands(raftCluster);
    }
}
