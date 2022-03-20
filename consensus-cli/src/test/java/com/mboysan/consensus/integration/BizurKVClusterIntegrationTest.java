package com.mboysan.consensus.integration;

import com.mboysan.consensus.BizurKVStoreCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BizurKVClusterIntegrationTest extends ClusterIntegrationTestBase {

    private BizurKVStoreCluster bizurCluster;

    @AfterEach
    void teardown() {
        bizurCluster.cleanup();
    }

    @Test
    void testKVOperationsSimpleSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(5)
                .build();
        testKVOperationsSimple(bizurCluster);
    }

    @Test
    void testKVOperationsSimpleMultiBuckets() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(5)
                .setNumNodes(5)
                .build();
        testKVOperationsSimple(bizurCluster);
    }

    @Test
    void testKVOperationsSequentialSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(5)
                .build();
        testKVOperationsSequential(bizurCluster);
    }

    @Test
    void testKVOperationsSequentialMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(5)
                .setNumNodes(5)
                .build();
        testKVOperationsSequential(bizurCluster);
    }

    @Test
    void testKVOperationsMultiThreadedSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(5)
                .build();
        testKVOperationsMultiThreaded(bizurCluster);
    }

    @Test
    void testKVOperationsMultiThreadedMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(5)
                .setNumNodes(5)
                .build();
        testKVOperationsMultiThreaded(bizurCluster);
    }

    @Test
    void testKVStoreShutdownAndStartSingleBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(1)
                .setNumNodes(5)
                .build();
        testKVStoreShutdownAndStart(bizurCluster);
    }

    @Test
    void testKVStoreShutdownAndStartMultiBucket() throws Exception {
        this.bizurCluster = new BizurKVStoreCluster.Builder()
                .setNumBuckets(5)
                .setNumNodes(5)
                .build();
        testKVStoreShutdownAndStart(bizurCluster);
    }
}
