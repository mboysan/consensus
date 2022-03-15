package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedRunnable;
import com.mboysan.consensus.util.NetUtil;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.StringJoiner;

public abstract class KVStoreClusterBase {

    static {
        KVStoreClientCLI.testingInProgress = true;
    }

    private static final SecureRandom RNG = new SecureRandom();

    int[][] ports(int numNodes) {
        int[][] ports = new int [numNodes][2];
        for (int i = 0; i < numNodes; i++) {
            ports[i][0] = NetUtil.findFreePort();   // node port
            ports[i][1] = NetUtil.findFreePort();   // store port
        }
        return ports;
    }

    String destinations(int[][] ports) {
        StringJoiner sj = new StringJoiner(",");
        for (int i = 0; i < ports.length; i++) {
            String nodeId = i + "";
            String address = "localhost:" + ports[i][0];
            sj.add(nodeId + "-" + address);
        }
        return sj.toString();    // it will look like -> 0-localhost:8080,1-localhost:8081 ...
    }


    public AbstractKVStore<?> getStore(int nodeId) {
        return KVStoreServerCLI.STORE_REFERENCES.get(nodeId);
    }

    public AbstractNode<?> getNode(int nodeId) {
        return NodeCLI.NODE_REFERENCES.get(nodeId);
    }

    public KVStoreClient getClient(int clientId) {
        return KVStoreClientCLI.CLIENT_REFERENCES.get(clientId);
    }

    public KVStoreClient getRandomClient() {
        return getClient(randomClientId());
    }

    public Collection<KVStoreClient> getClients() {
        return KVStoreClientCLI.CLIENT_REFERENCES.values();
    }

    public int randomClientId() {
        return RNG.nextInt(getClients().size());
    }

    public void cleanup() {
        KVStoreClientCLI.CLIENT_REFERENCES.forEach((i, client) -> client.shutdown());
        NodeCLI.NODE_REFERENCES.forEach((i, node) -> node.shutdown());
        KVStoreServerCLI.STORE_REFERENCES.forEach((i, store) -> store.shutdown());
    }

    static Thread exec(CheckedRunnable<Exception> runnable) {
        return new Thread(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
