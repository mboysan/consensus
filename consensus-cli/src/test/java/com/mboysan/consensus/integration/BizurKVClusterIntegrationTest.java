package com.mboysan.consensus.integration;

import com.mboysan.consensus.BizurKVStoreCluster;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BizurKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private BizurKVStoreCluster bizurCluster;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @AfterEach
    void teardown() {
        bizurCluster.cleanup();
    }

    @Test
    void testKVOperationsSimpleSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(3)
                .build();
        testKVOperationsSimple(bizurCluster);
    }

    @Test
    void testKVOperationsSimpleMultiBuckets() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();
        testKVOperationsSimple(bizurCluster);
    }

    @Test
    void testKVOperationsSequentialSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(3)
                .build();
        testKVOperationsSequential(bizurCluster);
    }

    @Test
    void testKVOperationsSequentialMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();
        testKVOperationsSequential(bizurCluster);
    }

    @Test
    void testKVOperationsMultiThreadedSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(3)
                .build();
        testKVOperationsMultiThreaded(bizurCluster);
    }

    @Test
    void testKVOperationsMultiThreadedMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();
        testKVOperationsMultiThreaded(bizurCluster);
    }

    @Test
    void testKVStoreShutdownAndStartSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(3)
                .build();
        testKVStoreShutdownAndStart(bizurCluster);
    }

    @Test
    void testKVStoreShutdownAndStartMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();
        testKVStoreShutdownAndStart(bizurCluster);
    }

    @Test
    void testKVStoreLeaderFailure() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();

        // key 'a' belongs to bucket-2 which belongs to node-2 (leader).
        final String expectedKey = "a";
        final String expectedValue = "v0";

        bizurCluster.getClient(0).set(expectedKey, expectedValue);
        bizurCluster.getStore(2).shutdown();

        String actualValue = awaiting(() -> bizurCluster.getClient(0).get(expectedKey));
        assertEquals(expectedValue, actualValue);
    }

    @Test
    void testCustomCommands() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(3)
                .setNumNodes(3)
                .build();
        testCustomCommands(bizurCluster);
    }
}
