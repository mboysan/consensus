package com.mboysan.consensus;

class BizurKVStoreTest extends KVStoreTestBase<BizurNode> implements BizurInternals {
    @Override
    KVStore createKVStore(BizurNode node) {
        return new BizurKVStore(node);
    }
}
