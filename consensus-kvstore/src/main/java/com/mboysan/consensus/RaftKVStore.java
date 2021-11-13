package com.mboysan.consensus;

import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.StateMachineRequest;
import com.mboysan.consensus.message.StateMachineResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class RaftKVStore implements KVStoreRPC {

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
    public Future<Void> start() throws IOException {
        return raft.start();
    }

    @Override
    public synchronized void shutdown() {
        raft.shutdown();
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            String value = store.get(Objects.requireNonNull(request.getKey()));
            return new KVGetResponse(true, null, value).responseTo(request);
        } catch (Exception e) {
            logError(raft.getNodeId(), request, e);
            return new KVGetResponse(false, e, null).responseTo(request);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            String key = Objects.requireNonNull(request.getKey());
            String value = Objects.requireNonNull(request.getValue());
            boolean success = append(String.format("put%s%s%s%s", CMD_SEP, key, CMD_SEP, value), request);
            return new KVSetResponse(success, null).responseTo(request);
        } catch (Exception e) {
            logError(raft.getNodeId(), request, e);
            return new KVSetResponse(false, e).responseTo(request);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            String key = Objects.requireNonNull(request.getKey());
            boolean success = append(String.format("rm%s%s", CMD_SEP, key), request);
            return new KVDeleteResponse(success, null).responseTo(request);
        } catch (Exception e) {
            logError(raft.getNodeId(), request, e);
            return new KVDeleteResponse(false, e).responseTo(request);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            Set<String> keys = new HashSet<>(store.keySet());
            return new KVIterateKeysResponse(true, null, keys).responseTo(request);
        } catch (Exception e) {
            logError(raft.getNodeId(), request, e);
            return new KVIterateKeysResponse(false, e, null).responseTo(request);
        }
    }

    private boolean append(String command, Message baseRequest) throws IOException {
        StateMachineRequest request = new StateMachineRequest(command)
                .setSenderId(baseRequest.getSenderId())
                .setReceiverId(baseRequest.getReceiverId())
                .setCorrelationId(baseRequest.getCorrelationId());
        StateMachineResponse response = raft.stateMachineRequest(request);
        return response.isApplied();
    }

    private static void logError(int nodeId, Message request, Exception err) {
        LOGGER.error("on RaftKVStore-{}, error={}, for request={}", nodeId, err, request.toString());
    }
}
