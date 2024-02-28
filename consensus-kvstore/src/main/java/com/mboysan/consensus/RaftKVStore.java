package com.mboysan.consensus;

import com.mboysan.consensus.event.MeasurementEvent;
import com.mboysan.consensus.event.MeasurementEvent.MeasurementType;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
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
    void dumpStoreMetricsAsync() {
        final long sizeOfKeys = store.keySet().stream().mapToLong(k -> k.length()).sum();
        final long sizeOfValues = store.values().stream().mapToLong(v -> v.length()).sum();
        final long totalSize = sizeOfKeys + sizeOfValues;

        fireMeasurementAsync(Constants.Metrics.INSIGHTS_STORE_SIZE_OF_KEYS, sizeOfKeys);
        fireMeasurementAsync(Constants.Metrics.INSIGHTS_STORE_SIZE_OF_VALUES, sizeOfValues);
        fireMeasurementAsync(Constants.Metrics.INSIGHTS_STORE_SIZE_OF_TOTAL, totalSize);
    }

    private void fireMeasurementAsync(String name, long value) {
        EventManagerService.getInstance().fireAsync(new MeasurementEvent(MeasurementType.SAMPLE, name, value));
    }

    @Override
    public KVGetResponse get(KVGetRequest request) {
        try {
            String key = request.getKey();
            boolean success = append(String.format("get%s%s", CMD_SEP, key), request);
            String value = store.get(Objects.requireNonNull(request.getKey()));
            return new KVGetResponse(success, null, value);
        } catch (Exception e) {
            logError(request, e);
            return new KVGetResponse(false, e, null);
        }
    }

    @Override
    public KVSetResponse set(KVSetRequest request) {
        try {
            String key = Objects.requireNonNull(request.getKey());
            String value = Objects.requireNonNull(request.getValue());
            boolean success = append(String.format("put%s%s%s%s", CMD_SEP, key, CMD_SEP, value), request);
            return new KVSetResponse(success, null);
        } catch (Exception e) {
            logError(request, e);
            return new KVSetResponse(false, e);
        }
    }

    @Override
    public KVDeleteResponse delete(KVDeleteRequest request) {
        try {
            String key = Objects.requireNonNull(request.getKey());
            boolean success = append(String.format("rm%s%s", CMD_SEP, key), request);
            return new KVDeleteResponse(success, null);
        } catch (Exception e) {
            logError(request, e);
            return new KVDeleteResponse(false, e);
        }
    }

    @Override
    public KVIterateKeysResponse iterateKeys(KVIterateKeysRequest request) {
        try {
            boolean success = append("iterateKeys", request);
            Set<String> keys = new HashSet<>(store.keySet());
            return new KVIterateKeysResponse(success, null, keys);
        } catch (Exception e) {
            logError(request, e);
            return new KVIterateKeysResponse(false, e, null);
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

    @Override
    public CustomResponse customRequest(CustomRequest request) {
        try {
            return getNode().customRequest(request);
        } catch (Exception e) {
            logError(request, e);
            return new CustomResponse(false, e, null);
        }
    }
}
