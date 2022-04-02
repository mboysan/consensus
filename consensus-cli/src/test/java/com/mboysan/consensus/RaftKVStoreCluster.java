package com.mboysan.consensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RaftKVStoreCluster extends KVStoreClusterBase {

    private RaftKVStoreCluster(Builder builder) throws IOException, InterruptedException {
        List<Thread> threads = new ArrayList<>();

        int[][] ports = ports(builder.numNodes);
        String destinations = destinations(ports);

        for (int i = 0; i < builder.numNodes; i++) {
            String[] storeArgs = new String[] {
                    "--node",
                    "node.id=%d".formatted(i),
                    "protocol=raft",
                    "port=" + ports[i][0],  // nodes will connect to this node
                    "destinations=" + destinations,

                    "--store",
                    "port=" + ports[i][1],  // clients will connect to this port
            };
            threads.add(newThread(() -> KVStoreServerCLI.main(storeArgs)));

            String storeDestination = "%d-localhost:%d".formatted(i, ports[i][1]);
            String[] clientArgs = new String[]{
                    "client.id=" + i,
                    "destinations=" + storeDestination
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

        public Builder setNumNodes(int numNodes) {
            this.numNodes = numNodes;
            return this;
        }

        public RaftKVStoreCluster build() throws IOException, InterruptedException {
            return new RaftKVStoreCluster(this);
        }
    }
}
