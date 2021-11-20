package com.mboysan.consensus;

class BizurKVStoreTest extends KVStoreTestBase<BizurNode> implements BizurInternals {
    @Override
    BizurKVStore createKVStore(BizurNode node, Transport clientServingTransport) {
        return new BizurKVStore(node, clientServingTransport);
    }
}
