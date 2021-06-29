package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.KVStore;
import com.mboysan.dist.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class RaftKVStore implements KVStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftKVStore.class);

    private static final String CMD_SEP = "@@@";

    private final RaftServer raft;
    private final Map<String, String> store = new ConcurrentHashMap<>();

    private final Consumer<String> stateMachine = cmd -> {
        LOGGER.debug("applying command={}", cmd);
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

    public RaftKVStore(int nodeId, Transport transport) {
        this(new RaftServer(nodeId, transport));
    }

    RaftKVStore(RaftServer raft) {
        this.raft = raft;
        raft.registerStateMachine(stateMachine);
    }

    @Override
    public synchronized void start() throws Exception {
        if (raft.isRunning()) {
            return;
        }
        raft.start().get();
    }

    @Override
    public synchronized void shutdown() throws IOException {
        if (!raft.isRunning()) {
            return;
        }
        raft.shutdown();
    }

    @Override
    public boolean put(String key, String value) {
        validateAction();
        return append(String.format("put%s%s%s%s", CMD_SEP, key, CMD_SEP, value));
    }

    @Override
    public String get(String key) {
        validateAction();
        return store.get(key);
    }

    @Override
    public boolean remove(String key) {
        validateAction();
        return append(String.format("rm%s%s", CMD_SEP, key));
    }

    public int size() {
        return store.size();
    }

    public Set<String> keySet() {
        return store.keySet();
    }

    private boolean append(String command) {
        try {
            return raft.append(command).get();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    private void validateAction() {
        if (!raft.isRunning()) {
            throw new IllegalStateException("raft not running");
        }
    }
}
