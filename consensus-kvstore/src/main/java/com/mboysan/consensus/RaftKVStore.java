package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class RaftKVStore implements KVStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVStore.class);

    private static final String CMD_SEP = "@@@";

    private final RaftNode raft;
    private final Map<String, String> store = new ConcurrentHashMap<>();

    private final Consumer<String> stateMachine = cmd -> {
        String[] split = cmd.split(CMD_SEP);
        String command = split[0];
        if (command.equals("put")) {
            String key = split[1];
            String val = split[2];
            store.put(key, val);
        } else if (command.equals("rm")) {
            String key = split[1];
            store.remove(key);
        }
    };

    public RaftKVStore(RaftNode raft) {
        this.raft = raft;
        raft.registerStateMachine(stateMachine);
    }

    @Override
    public synchronized void start() throws Exception {
        raft.start().get();
    }

    @Override
    public synchronized void shutdown() {
        raft.shutdown();
    }

    @Override
    public boolean put(String key, String value) throws KVOperationException {
        return append(String.format("put%s%s%s%s", CMD_SEP, key, CMD_SEP, value));
    }

    @Override
    public String get(String key) {
        return store.get(key);
    }

    @Override
    public boolean remove(String key) throws KVOperationException {
        return append(String.format("rm%s%s", CMD_SEP, key));
    }

    @Override
    public Set<String> keySet() {
        return store.keySet();
    }

    private boolean append(String command) throws KVOperationException {
        try {
            boolean applied = raft.append(command).get(5, TimeUnit.SECONDS);
            if (!applied) {
                throw new KVOperationException("append failed for cmd=" + command);
            }
            return true;
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
            throw new KVOperationException(e);
        } catch (ExecutionException | TimeoutException e) {
            LOGGER.error(e.getMessage(), e);
            throw new KVOperationException(e);
        }
    }
}
