package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.message.CheckRaftIntegrityRequest;
import com.mboysan.consensus.message.StateMachineRequest;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static com.mboysan.consensus.util.AwaitUtil.awaitingAtLeast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftNodeTest extends NodeTestBase {

    private boolean skipTeardown;
    private RaftNode[] nodes;
    private InVMTransport transport;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    void initCluster(int numNodes) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new RaftNode[numNodes];
        transport = new InVMTransport();
        for (int i = 0; i < numNodes; i++) {
            RaftConfig raftConfig = raftConfig(i);
            RaftNode node = new RaftNode(raftConfig, transport);
            nodes[i] = node;

            futures.add(node.start());
        }

        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private RaftConfig raftConfig(int nodeId) {
        Properties properties = new Properties();
        properties.put(NodeConfig.Param.NODE_ID, nodeId + "");
        properties.put(RaftConfig.Param.UPDATE_INTERVAL_MS, 50 + "");
        properties.put(RaftConfig.Param.ELECTION_TIMEOUT_MS, 100 + "");
        return CoreConfig.newInstance(RaftConfig.class, properties);
    }

    @Override
    InVMTransport getTransport() {
        return transport;
    }

    @Override
    RaftNode getNode(int nodeId) {
        return nodes[nodeId];
    }

    @Test
    void testWhenServerNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        RaftNode node = new RaftNode(raftConfig(0), transport);

        StateMachineRequest request = new StateMachineRequest("some-command");
        assertThrows(IllegalStateException.class, () -> node.stateMachineRequest(request));
        skipTeardown = true;
    }

    @Test
    void testLeaderElected() throws Exception {
        initCluster(3);
        int leaderId = this.assertOneLeader();

        // wait a while and check again to see if leader remained unchanged
        awaitingAtLeast(2000L, () -> {
            assertEquals(leaderId, assertOneLeader());
            awaiting(this::assertIntegrityCheckPassed);
        });
    }

    /**
     * Tests re-election mechanism after a leader is killed/disconnected.
     */
    @Test
    void testReElection() throws Exception {
        initCluster(3);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);

        // wait a while and check if the leader has changed
        awaiting(() -> assertLeaderChanged(oldLeaderId, oldLeaderId /* oldLeader is not aware */));

        // rejoin old leader, old leader might try to recover its leadership due to its short update interval
        // but at the end there must still be one leader
        connect(oldLeaderId);
        awaiting(() -> {
            assertOneLeader();
            assertIntegrityCheckPassed();
        });
    }

    /**
     * Tests when the minority number of followers are killed, then the old leader still remains as the leader after
     * the dead followers are revived.
     */
    @Test
    void testLeaderNotChanged() throws Exception {
        int numServers = 5;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        int follower1 = (leaderId + 1) % numServers;
        int follower2 = (leaderId + 2) % numServers;

        kill(follower1);
        kill(follower2);

        assertLeaderNotChanged(leaderId, follower1, follower2); // not visible by the followers.

        revive(follower1);
        revive(follower2);

        assertLeaderNotChanged(leaderId);   // leader is visible by everyone.
        awaiting(() -> {
            assertOneLeader();
            assertIntegrityCheckPassed();
        });
    }

    /**
     * Tests appending log entries when everything is running smoothly.
     */
    @Test
    void testWhenAllConnectedThenAppendSucceeds() throws Exception {
        initCluster(3);
        int leaderId = assertOneLeader();

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1", "cmd3");
        assertTrue(append(leaderId, expectedCommands.get(0)));
        assertTrue(append(leaderId, expectedCommands.get(1)));
        assertTrue(append(leaderId, expectedCommands.get(2)));
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);

        // everything should stay the same even after some time passes
        awaitingAtLeast(2000L, () -> {
            assertLeaderNotChanged(leaderId);
            assertLogsEquals(expectedCommands);
            awaiting(this::assertIntegrityCheckPassed);
        });
    }

    /**
     * Tests the failure of an append log entry event when the leader is killed/disconnected and there was no time
     * to elect a new one.
     */
    @Test
    void testWhenLeaderDisconnectedThenAppendFails() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        disconnect(leaderId);
        assertThrows(IOException.class, () -> append((leaderId + 1) % numServers, "some-command"));
    }

    /**
     * Tests if a command will be routed to the leader if the node is a follower.
     */
    @Test
    void testCommandRouting() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1", "cmd2");
        // if any of the following node is a follower, command will be routed to leader
        assertTrue(append((leaderId + 1) % numServers, expectedCommands.get(0)));
        assertTrue(append((leaderId + 2) % numServers, expectedCommands.get(1)));
        assertTrue(append((leaderId + 3) % numServers, expectedCommands.get(2)));
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);

        // everything should stay the same even after some time passes
        awaitingAtLeast(2000L, () -> {
            assertLeaderNotChanged(leaderId);
            assertLogsEquals(expectedCommands);
            awaiting(this::assertIntegrityCheckPassed);
        });
    }

    /**
     * Tests if a follower node is down, the system is still operational.
     */
    @Test
    void testFollowerFailure() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        kill((leaderId + 1) % numServers);

        List<String> expectedCommands = List.of("cmd0");
        assertTrue(append(leaderId, expectedCommands.get(0)));

        revive((leaderId + 1) % numServers);

        // allow sync time
        awaiting(() -> {
            assertLeaderNotChanged(leaderId);
            assertLogsEquals(expectedCommands);
            awaiting(this::assertIntegrityCheckPassed);
        });
    }

    /**
     * Tests when a leader fails, a new leader will be elected and the old leader will sync with all the changes
     * in the system state.
     */
    @Test
    void testLeaderFailure1() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int oldLeaderId = assertOneLeader();

        kill(oldLeaderId);

        int newLeaderId = awaiting(() -> assertLeaderChanged(oldLeaderId)); // a new leader will be elected

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1");
        assertTrue(append(newLeaderId, expectedCommands.get(0)));

        revive(oldLeaderId);

        assertTrue(append(newLeaderId, expectedCommands.get(1)));
        // old leader will pick up all the changes during the above command update

        assertLeaderNotChanged(newLeaderId);
        assertLogsEquals(expectedCommands);
        awaiting(this::assertIntegrityCheckPassed);
    }

    /**
     * Tests when a leader fails, a new leader will be elected and the old leader will sync with all the changes
     * in the system state.
     */
    @Test
    void testLeaderFailure2() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);

        // a new leader will be elected
        int newLeaderId = awaiting(() -> assertLeaderChanged(oldLeaderId, oldLeaderId /* oldLeader is not aware */));

        List<String> expectedCommands = List.of("cmd0");
        assertTrue(append(newLeaderId, expectedCommands.get(0)));

        connect(oldLeaderId);

        // sync changes
        awaiting(() -> {
            assertOneLeader();
            assertLogsEquals(expectedCommands);
            assertIntegrityCheckPassed();
        });
    }

    /**
     * Tests append event during a broken quorum. Append will fail.
     */
    @Test
    void testAppendWhenQuorumNotFormed1() throws Exception {
        int numServers = 3;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        disconnect((leaderId + 1) % numServers);
        disconnect((leaderId + 2) % numServers);

        // break the quorum and try to append a command, progress cannot be made hence

        boolean result = append(leaderId, "cmd0");
        assertFalse(result);
    }

    /**
     * Tests append event during a broken quorum and the leader is changed after the quorum is reestablished.
     * Append will fail.
     */
    @Test
    void testAppendWhenQuorumNotFormed2() throws Exception {
        int numServers = 5;
        initCluster(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect((oldLeaderId + 4) % numServers);
        disconnect((oldLeaderId + 3) % numServers);
        disconnect((oldLeaderId + 2) % numServers);

        List<String> expectedCommands = new ArrayList<>(List.of("cmd0"));

        boolean result = append(oldLeaderId, expectedCommands.get(0));
        assertFalse(result);

        kill(oldLeaderId);
        connect((oldLeaderId + 4) % numServers);
        connect((oldLeaderId + 3) % numServers);
        connect((oldLeaderId + 2) % numServers);
        awaiting(() -> assertOneLeader());

        // There is an interesting edge-case here. Assuming the oldLeader was node-0 and the node that was NOT
        // disconnected was node-1: After append(cmd0) above, node-0 will send the AppendEntries request to node-1.
        // This entry is not yet committed on any of the nodes but node-1 has this entry in its log. If by any chance
        // node-1 is elected as the leader, this entry will be replicated to its followers. At this point, the clients
        // will be very confused, because the result of the append(cmd0) request was false, but the entry is committed.
        // Raft paper addresses this problem in the "Client Interaction" section, i.e. clients always send a
        // unique id with each request. If the leader receives a request with a duplicate id,
        // it will reject the request.
        //
        // That's why at the end of the test, instead of checking the logs for expectedCommands, we are just checking
        // if the log entries on all the nodes are equal or not.

        revive(oldLeaderId);
        awaiting(() -> {
            // old leader will sync changes
            assertOneLeader();
            assertNodeLogsEquals();
            assertIntegrityCheckPassed();
        });
    }

    /**
     * Tests append event during a broken quorum where one of the disconnected nodes is the leader. The leader will
     * try to write a new entry (cmd0) which will eventually be discarded.
     * While the leader's network connection is still down and the follower connections are repaired a new leader will
     * be elected. The new leader will write a new entry and when the old leader's connection is repaired, it sees
     * the new leader and this new entry (cmd1).
     */
    @Test
    void testAppendWhenQuorumNotFormed3() throws Exception {
        int numServers = 5;
        initCluster(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);
        disconnect((oldLeaderId + 2) % numServers);
        disconnect((oldLeaderId + 3) % numServers);

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1");
        boolean result = append(oldLeaderId, expectedCommands.get(0));
        assertFalse(result);    // the "cmd0" entry will not be applied at all

        connect((oldLeaderId + 2) % numServers);
        connect((oldLeaderId + 3) % numServers);

        int newLeaderId = awaiting(() -> assertLeaderChanged(oldLeaderId, oldLeaderId /* oldLeader is not aware */));
        assertTrue(append(newLeaderId, expectedCommands.get(1))); // append a new entry to log. oldLeader's entry will not be synced.

        connect(oldLeaderId);  // connect old leader and discover new one
        awaiting(() -> assertOneLeader());

        expectedCommands = List.of("cmd1");
        assertLogsEquals(expectedCommands); // log item will be applied as soon as the quorum is formed again.
        awaiting(this::assertIntegrityCheckPassed);
    }

    /**
     * When a follower disconnects, it will not be able to receive the append event. Instead, it will try to start
     * a new election (increasing term), and will reject all the AppendEntries requests with this new term. However,
     * other nodes will not recognize its leadership because of log inconsistencies. Therefore, eventually (after
     * enough time passes) all nodes will possibly elect a new leader and sync the logs.
     */
    @Test
    void testFollowerDisconnectsDuringAppend() throws Exception {
        int numServers = 5;
        initCluster(numServers);
        int leaderId = assertOneLeader();

        disconnect((leaderId + 1) % numServers);
        disconnect((leaderId + 2) % numServers);
        List<String> expectedCommands = List.of("cmd0");
        append(leaderId, expectedCommands.get(0));

        connect((leaderId + 1) % numServers);
        connect((leaderId + 2) % numServers);

        awaiting(() -> {
            assertOneLeader();  // leader might've changed but there must still be only one leader.
            assertLogsEquals(expectedCommands);
            assertIntegrityCheckPassed();
        });
    }

    // ------------------------------------------------------------- assertions

    private void assertIntegrityCheckPassed() throws IOException {
        for (RaftNode node : nodes) {
            assertTrue(checkIntegrity(node.getNodeId()));
        }
    }

    private void assertLogsEquals(List<String> commands) {
        RaftLog log0;
        synchronized (nodes[0]) {
            log0 = nodes[0].getState().raftLog;
            assertEquals(commands.size(), log0.size());
            for (int i = 0; i < log0.size(); i++) {
                assertEquals(commands.get(i), log0.get(i).command());
            }
        }
        assertNodeLogsEquals();
    }

    private void assertNodeLogsEquals() {
        for (RaftNode server : nodes) {
            synchronized (server) {
                RaftLog log0 = nodes[0].getState().raftLog;
                assertEquals(log0, server.getState().raftLog);
            }
        }
    }

    /**
     * Asserts that the leader id is not changed, excludes the node-ids provided with <tt>changeInvisibleOnNodeIds</tt>
     * parameter.
     * @param currentLeaderId          node id of the current leader
     * @param changeInvisibleOnNodeIds node ids that the leader changes are not visible
     */
    private void assertLeaderNotChanged(int currentLeaderId, int... changeInvisibleOnNodeIds) {
        assertEquals(currentLeaderId, assertOneLeader(changeInvisibleOnNodeIds));
    }

    /**
     * Asserts that the leader is changed, excludes the node-ids provided with <tt>changeInvisibleOnNodeIds</tt>
     * parameter.
     * @param oldLeader                node if of the old leader
     * @param changeInvisibleOnNodeIds node ids that the leader changes are not visible
     * @return new leader id
     */
    private int assertLeaderChanged(int oldLeader, int... changeInvisibleOnNodeIds) {
        int newLeaderId = assertOneLeader(changeInvisibleOnNodeIds);
        assertNotEquals(oldLeader, newLeaderId);
        return newLeaderId;
    }

    /**
     * Asserts that there's only one leader across all the nodes, excluding the node-ids provided
     * with <tt>changeInvisibleOnNodeIds</tt> parameter.
     * @param changeInvisibleOnNodeIds node ids that the leader changes are not visible
     * @return id of the leader
     */
    private int assertOneLeader(int... changeInvisibleOnNodeIds) {
        int leaderId = -1;
        for (RaftNode node : nodes) {
            OptionalInt invisibleNode = Arrays.stream(changeInvisibleOnNodeIds)
                    .filter(invisibleNodeId -> invisibleNodeId == node.getNodeId())
                    .findFirst();
            if (invisibleNode.isPresent()) {
                continue;
            }
            synchronized (node) {
                if (leaderId == -1) {
                    leaderId = node.getState().leaderId;
                }
                assertEquals(leaderId, node.getState().leaderId);
            }
        }
        assertNotEquals(-1, leaderId);
        return leaderId;
    }

    // ------------------------------------------------------------- node operations

    private boolean append(int nodeId, String command) throws IOException {
        return nodes[nodeId].stateMachineRequest(new StateMachineRequest(command)).isApplied();
    }

    private boolean checkIntegrity(int nodeId) throws IOException {
        return nodes[nodeId].checkRaftIntegrity(
                new CheckRaftIntegrityRequest(CheckRaftIntegrityRequest.Level.STATE_FROM_ALL)).isSuccess();

    }

    @AfterEach
    void tearDown() {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(RaftNode::shutdown);
        transport.shutdown();
    }
}
