package com.mboysan.consensus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CLIBase {
    static volatile boolean testingInProgress = false;
    static final Map<Integer, AbstractKVStore<?>> STORE_REFERENCES = new ConcurrentHashMap<>();
    static final Map<Integer, AbstractNode<?>> NODE_REFERENCES = new ConcurrentHashMap<>();
    static final Map<Integer, KVStoreClient> CLIENT_REFERENCES = new ConcurrentHashMap<>();
}
