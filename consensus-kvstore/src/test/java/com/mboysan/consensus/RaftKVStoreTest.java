package com.mboysan.consensus;

class RaftKVStoreTest extends KVStoreTestBase<RaftNode> implements RaftInternals {
    @Override
    RaftKVStore createKVStore(RaftNode node, Transport clientServingTransport) {
        return new RaftKVStore(node, clientServingTransport);
    }
}
