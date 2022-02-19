package com.mboysan.consensus;

import com.mboysan.consensus.message.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class RaftKVStore extends AbstractKVStore<RaftNode> {

    private static final String CMD_SEP = "@@@";

    private final Map<String, String> store = new ConcurrentHashMap<>();

    public RaftKVStore(RaftNode node, Transport clientServingTransport) {
        super(node, clientServingTransport);

        Consumer<String> stateMachine = cmd -> {
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

        getNode().registerStateMachine(stateMachine);
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            String key = request.getKey();
            boolean success = append(String.format("get%s%s", CMD_SEP, key), request);
            String value = store.get(Objects.requireNonNull(request.getKey()));
            return new KVGetResponse(success, null, value).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
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
            logError(request, e);
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
            logError(request, e);
            return new KVDeleteResponse(false, e).responseTo(request);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            boolean success = append("iterateKeys", request);
            Set<String> keys = new HashSet<>(store.keySet());
            return new KVIterateKeysResponse(success, null, keys).responseTo(request);
        } catch (Exception e) {
            logError(request, e);
            return new KVIterateKeysResponse(false, e, null).responseTo(request);
        }
    }

    private boolean append(String command, Message baseRequest) throws IOException {
        StateMachineRequest request = new StateMachineRequest(command)
                .setSenderId(baseRequest.getSenderId())
                .setReceiverId(baseRequest.getReceiverId())
                .setCorrelationId(baseRequest.getCorrelationId());
        StateMachineResponse response = getNode().stateMachineRequest(request);
        return response.isApplied();
    }
}
