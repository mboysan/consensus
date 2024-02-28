package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.message.SimMessage;
import com.mboysan.consensus.util.RngUtil;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.mboysan.consensus.SimState.Role.FOLLOWER;
import static com.mboysan.consensus.SimState.Role.LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SimNodeTest extends NodeTestBase {

    private SimNode[] nodes;

    private InVMTransport transport;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    void initCluster(TestConfig testConfig) throws IOException, ExecutionException, InterruptedException {
        final int numNodes = testConfig.numNodes;
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new SimNode[numNodes];
        transport = new InVMTransport();
        for (int i = 0; i < numNodes; i++) {
            SimConfig simConfig = simConfig(i, testConfig.asProperties());
            SimNode node = new SimNode(simConfig, transport);
            nodes[i] = node;

            futures.add(node.start());
        }

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private SimConfig simConfig(int nodeId, Properties properties) {
        properties.put(NodeConfig.Param.NODE_ID, nodeId + "");
        return CoreConfig.newInstance(SimConfig.class, properties);
    }

    @Override
    InVMTransport getTransport() {
        return transport;
    }

    @Override
    SimNode getNode(int nodeId) {
        return nodes[nodeId];
    }

    @Test
    void testRoles() throws Exception {
        final int numNodes = 3;
        final TestConfig testConfig = new TestConfig(numNodes);
        initCluster(testConfig);
        int leaderId = testConfig.leaderId;
        for (SimNode node : nodes) {
            // every node must recognize the same node as the leader
            assertEquals(leaderId, node.getState().getLeaderId());

            if (leaderId == node.getNodeId()) {
                assertEquals(LEADER, node.getState().getRole());
            } else {
                assertEquals(FOLLOWER, node.getState().getRole());
            }
        }
    }

    @Test
    void testSimulateLeaderBroadcastsToFollowers() throws Exception {
        final TestConfig testConfig = new TestConfig(3).setBroadcastToFollowers(true);
        initCluster(testConfig);

        simulate(testConfig.leaderId);

        for (SimNode node : nodes) {
            // leader receives 1 message, forwards it to followers. Followers receive 1 message from leader.
            assertEquals(1, node.getState().getMessageReceiveCount().get());
        }
    }

    @Test
    void testSimulateLeaderDoesNotBroadcastToFollowers() throws Exception {
        final TestConfig testConfig = new TestConfig(3).setBroadcastToFollowers(false);
        initCluster(testConfig);

        simulate(testConfig.leaderId);

        for (SimNode node : nodes) {
            // leader receives 1 message, but doesn't forward it to followers. Followers will not receive any message.
            if (LEADER.equals(node.getState().getRole())) {
                assertEquals(1, node.getState().getMessageReceiveCount().get());
            } else {
                assertEquals(0, node.getState().getMessageReceiveCount().get());
            }
        }
    }

    @Test
    void testSimulateFollowerForwardsToLeader() throws Exception {
        final int numNodes = 3;
        final TestConfig testConfig = new TestConfig(numNodes).setForwardToLeader(true);
        initCluster(testConfig);

        final int sender = (testConfig.leaderId + 1) % numNodes;
        simulate(sender);

        for (SimNode node : nodes) {
            // follower (sender node) receives 1 message, forwards it to leader, leader sends 1 message to each follower.
            // follower receives another message from the leader.
            if (sender == node.getNodeId()) {
                assertEquals(2, node.getState().getMessageReceiveCount().get());
            } else {
                assertEquals(1, node.getState().getMessageReceiveCount().get());
            }
        }
    }

    @Test
    void testSimulateFollowerDoesNotForwardToLeader() throws Exception {
        final int numNodes = 3;
        final TestConfig testConfig = new TestConfig(numNodes).setForwardToLeader(false);
        initCluster(testConfig);

        final int sender = (testConfig.leaderId + 1) % numNodes;
        simulate(sender);

        for (SimNode node : nodes) {
            // follower (sender node) receives 1 message, others do not receive any message.
            if (sender == node.getNodeId()) {
                assertEquals(1, node.getState().getMessageReceiveCount().get());
            } else {
                assertEquals(0, node.getState().getMessageReceiveCount().get());
            }
        }
    }

    private void simulate(int sender) throws IOException {
        getNode(sender).simulate(new SimMessage());
    }

    private static final class TestConfig {
        final int numNodes;
        final int leaderId;
        Boolean forwardToLeader = null;
        Boolean broadcastToFollowers = null;

        public TestConfig(int numNodes) {
            this.numNodes = numNodes;
            this.leaderId = RngUtil.nextInt(numNodes);
        }

        public TestConfig setForwardToLeader(boolean forwardToLeader) {
            this.forwardToLeader = forwardToLeader;
            return this;
        }

        public TestConfig setBroadcastToFollowers(boolean broadcastToFollowers) {
            this.broadcastToFollowers = broadcastToFollowers;
            return this;
        }

        Properties asProperties() {
            Properties properties = new Properties();
            put(properties, "simulate.leaderId", leaderId);
            put(properties, "simulate.follower.forwardToLeader", forwardToLeader);
            put(properties, "simulate.leader.broadcastToFollowers", broadcastToFollowers);
            return properties;
        }

        void put(Properties properties, String key, Object value) {
            if (value != null) {
                properties.put(key, value + "");
            }
        }
    }
}