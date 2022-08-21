package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.message.SimMessage;
import org.junit.jupiter.api.Test;

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

    void initCluster(int numNodes) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new SimNode[numNodes];
        transport = new InVMTransport();
        for (int i = 0; i < numNodes; i++) {
            SimConfig simConfig = simConfig(i);
            SimNode node = new SimNode(simConfig, transport);
            nodes[i] = node;

            futures.add(node.start());
        }

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private SimConfig simConfig(int nodeId) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
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
        initCluster(3);
        for (SimNode node : nodes) {
            if (node.getNodeId() == 0) {
                assertEquals(LEADER, node.getState().getRole());
            } else {
                assertEquals(FOLLOWER, node.getState().getRole());
            }
        }
    }

    @Test
    void testSimulateLeaderToFollowers() throws Exception {
        initCluster(3);

        simulate(0);

        for (SimNode node : nodes) {
            // leader receives 1 message, forwards it to followers. Followers receive 1 message from leader.
            assertEquals(1, node.getState().getMessageReceiveCount().get());
        }
    }

    @Test
    void testSimulateFollowerToLeader() throws Exception {
        initCluster(3);

        simulate(1);

        for (SimNode node : nodes) {
            // follower-1 receives 1 message, forwards it to leader, leader sends 1 message to each follower.
            // follower-1 receives another message from the leader.
            if (node.getNodeId() == 1) {
                assertEquals(2, node.getState().getMessageReceiveCount().get());
            } else {
                assertEquals(1, node.getState().getMessageReceiveCount().get());
            }
        }
    }

    private void simulate(int sender) throws IOException {
        getNode(sender).simulate(new SimMessage());
    }
}