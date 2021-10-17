package com.mboysan.consensus;

import org.junit.jupiter.api.Test;

class BizurKVStoreTest extends KVStoreTestBase<BizurNode> implements BizurInternals {

    @Test
    void testPutGet() throws Exception {
        super.testPutGet();
    }

    @Test
    void testRemove() throws Exception {
        super.testRemove();
    }

    @Test
    void multiThreadTest() throws Exception {
        super.multiThreadTest();
    }

    @Test
    void testFollowerFailure() throws Exception {
        super.testFollowerFailure();
    }
}