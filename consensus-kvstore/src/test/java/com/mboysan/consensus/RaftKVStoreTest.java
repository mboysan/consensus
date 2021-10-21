package com.mboysan.consensus;

class RaftKVStoreTest extends KVStoreTestBase<RaftNode> implements RaftInternals {
    @Override
    KVStore createKVStore(RaftNode node) {
        return new RaftKVStore(node);
    }
}
