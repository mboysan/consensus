package com.mboysan.consensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BizurKVStoreCluster extends KVStoreClusterBase {

    private BizurKVStoreCluster(Builder builder) throws InterruptedException, IOException {
        List<Thread> threads = new ArrayList<>();

        int[][] ports = ports(builder.numNodes);
        String destinations = destinations(ports);

        for (int i = 0; i < builder.numNodes; i++) {
            String[] args = new String[] {
                    "node.id=%d".formatted(i),
                    "node.consensus.protocol=bizur",
                    "transport.tcp.server.ports=%d,%d".formatted(ports[i][0], ports[i][1]),  // nodes will connect to first port and client to second
                    "transport.tcp.destinations=%s".formatted(destinations),
                    "bizur.numPeers=%d".formatted(builder.numNodes),
                    "bizur.numBuckets=%d".formatted(builder.numBuckets),
            };
            threads.add(exec(() -> KVStoreServerCLI.main(args)));

            String storeDestination = "%d-localhost:%d".formatted(i, ports[i][1]);
            String[] clientArgs = new String[]{
                    "client.id=%d".formatted(i),
                    "transport.tcp.destinations=%s".formatted(storeDestination)
            };
            KVStoreClientCLI.main(clientArgs);
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public static class Builder {
        private int numNodes;
        private int numBuckets;

        public Builder setNumNodes(int numNodes) {
            this.numNodes = numNodes;
            return this;
        }

        public Builder setNumBuckets(int numBuckets) {
            this.numBuckets = numBuckets;
            return this;
        }

        public BizurKVStoreCluster build() throws IOException, InterruptedException {
            return new BizurKVStoreCluster(this);
        }
    }
}
