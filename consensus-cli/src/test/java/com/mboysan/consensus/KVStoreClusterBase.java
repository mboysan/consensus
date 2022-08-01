package com.mboysan.consensus;

import com.mboysan.consensus.util.NetUtil;
import com.mboysan.consensus.util.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.StringJoiner;

public abstract class KVStoreClusterBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClusterBase.class);

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

    public AbstractNode<?> getNode(int nodeId) {
        return NodeCLI.getNode(nodeId);
    }

    public AbstractKVStore<?> getStore(int nodeId) {
        return KVStoreServerCLI.getStore(nodeId);
    }

    public KVStoreClient getClient(int clientId) {
        return KVStoreClientCLI.getClient(clientId);
    }

    public KVStoreClient getRandomClient() {
        return getClient(randomClientId());
    }

    public Collection<KVStoreClient> getClients() {
        return KVStoreClientCLI.getClients();
    }

    public int randomClientId() {
        return RNG.nextInt(getClients().size());
    }

    public void cleanup() {
        KVStoreClientCLI.getClients().forEach(client -> exec(client::shutdown));
        NodeCLI.getNodes().forEach(node -> exec(node::shutdown));
        KVStoreServerCLI.getStores().forEach(store -> exec(store::shutdown));
    }

    static Thread newThread(ThrowingRunnable runnable) {
        return new Thread(() -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });
    }

    private static void exec(ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }
    }
}
