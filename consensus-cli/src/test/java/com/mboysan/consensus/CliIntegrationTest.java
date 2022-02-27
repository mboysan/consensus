package com.mboysan.consensus;

import com.mboysan.consensus.util.CheckedRunnable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CLI integration tests
 */
public class CliIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CliIntegrationTest.class);

    @BeforeAll
    static void beforeAll() {
        KVStoreClientCLI.testingInProgress = true;
    }

    @AfterEach
    void cleanup() {
        KVStoreClientCLI.CLIENT_REFERENCES.forEach((i, client) -> client.shutdown());
        NodeCLI.NODE_REFERENCES.forEach((i, node) -> node.shutdown());
        KVStoreServerCLI.STORE_REFERENCES.forEach((i, store) -> store.shutdown());
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
        List<Thread> threads = new ArrayList<>();

        threads.add(exec(() -> {
            // KV Store server with node-0 for the clients to connect
            KVStoreServerCLI.main(new String[]{
                    "node.id=0",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.ports=8080,9090",  // nodes will connect to first port and client to second
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });
        }));

        threads.add(exec(() -> {
            // node-1
            NodeCLI.main(new String[]{
                    "node.id=1",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8081",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });
        }));

        threads.add(exec(() -> {
            // node-2
            NodeCLI.main(new String[]{
                    "node.id=2",
                    "node.consensus.protocol=%s".formatted(protocol),
                    "transport.netty.port=8082",
                    "transport.netty.destinations=0-localhost:8080,1-localhost:8081,2-localhost:8082"
            });
        }));

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        LOGGER.info("all nodes started");

        // client
        KVStoreClientCLI.main(new String[]{
                "node.id=111",   // client's id
                "transport.netty.destinations=0-localhost:9090"
        });
        KVStoreClient client = KVStoreClientCLI.CLIENT_REFERENCES.get(111);

        String key = "testKey";
        String val = "testVal";
        client.set(key, val);
        assertEquals(val, client.get(key));
    }

    private static Thread exec(CheckedRunnable<Exception> runnable) {
        return new Thread(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
