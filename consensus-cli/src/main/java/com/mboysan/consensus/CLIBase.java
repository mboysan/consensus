package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CLIBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CLIBase.class);

    static volatile boolean testingInProgress = false;

    static final Map<Integer, AbstractKVStore<?>> STORE_REFERENCES = new ConcurrentHashMap<>();
    static final Map<Integer, AbstractNode<?>> NODE_REFERENCES = new ConcurrentHashMap<>();
    static final Map<Integer, KVStoreClient> CLIENT_REFERENCES = new ConcurrentHashMap<>();

    static void exec(CheckedRunnable<Exception> runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
