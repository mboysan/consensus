package com.mboysan.consensus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CLI integration tests
 */
public class CliIntegrationTest {

    @BeforeAll
    static void setupBeforeClass() {
        CLIBase.testingInProgress = true;
    }

    @Test
    void testRaftKVStoreBootstrapping() throws Exception {
        bootstrapAndTest("raft");
    }

    @Test
    void testBizurKVStoreBootstrapping() throws Exception {
//        bootstrapAndTest("bizur");
    }

    void bootstrapAndTest(String protocol) throws Exception {
        try {
            // KV Store server with node-0 for the clients to connect
            KVStoreServerCLI.main(new String[]{
                    "node.id=0",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.ports=8080,9090",  // nodes will connect to first port and client to second
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });

            // node-1
            NodeCLI.main(new String[]{
                    "node.id=1",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8081",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });

            // node-2
            NodeCLI.main(new String[]{
                    "node.id=2",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8082",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });

            KVStoreServerCLI.sync();
            NodeCLI.sync();

            // client
            KVStoreClientCLI.main(new String[]{
                    "node.id=111",   // client's id
                    "transport.netty.destinations=0-localhost:9090"
            });
            KVStoreClient client = CLIBase.CLIENT_REFERENCES.get(111);

            String key = "testKey";
            String val = "testVal";
            client.set(key, val);
            assertEquals(val, client.get(key));

        } finally {
            CLIBase.CLIENT_REFERENCES.forEach((i, client) -> client.shutdown());
            CLIBase.NODE_REFERENCES.forEach((i, node) -> node.shutdown());
            CLIBase.STORE_REFERENCES.forEach((i, store) -> store.shutdown());
        }
    }
}
