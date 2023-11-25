package com.mboysan.consensus;

import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NodeCLIPropsFileTest extends KVStoreClusterBase {

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @Test
    void testLoadPropertiesFromFile() throws IOException, ExecutionException, InterruptedException {
        int expectedNodeId = 123;
        String expectedConsensusProtocol = "raft";

        int[][] ports = ports(1);
        int port = ports[0][0];
        String destinations = destinations(ports);
        NodeCLI.main(new String[] {
                // read node.id from properties file
                "propsFile=classpath:node-cli-test.properties",

                // other properties will be inline
                "protocol=" + expectedConsensusProtocol,
                "port=" + port,
                "destinations=" + destinations
        });
        RaftNode node = null;
        try {
            node = (RaftNode) getNode(expectedNodeId);
        } finally {
            if (node != null) {
                node.shutdown();
            }
        }
        assertNotNull(node);
        assertEquals(expectedNodeId, node.getNodeId());
        assertEquals(expectedConsensusProtocol, node.getConfiguration().nodeConsensusProtocol());
    }

    @AfterEach
    void teardown() {
        cleanup();
    }

}
