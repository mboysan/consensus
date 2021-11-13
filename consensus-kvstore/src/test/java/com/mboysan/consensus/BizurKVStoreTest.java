package com.mboysan.consensus;

class BizurKVStoreTest extends KVStoreTestBase<BizurNode> implements BizurInternals {
    @Override
    BizurKVStore createKVStore(BizurNode node) {
        return new BizurKVStore(node);
    }
}
