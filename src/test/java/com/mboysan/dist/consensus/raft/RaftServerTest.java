package com.mboysan.dist.consensus.raft;

import com.mboysan.dist.InVMTransport;
import com.mboysan.dist.Transport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class RaftServerTest extends RaftTestBase {

    @Test
    void testWhenServerNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        RaftServer node = new RaftServer(0, transport);
        assertThrows(IllegalStateException.class, () -> {
            try {
                node.append("some-command").get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
        node.shutdown();
        transport.shutdown();
        skipTeardown = true;
    }

    @Test
    void testLeaderElected() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        // wait a while and check again to see if leader remained unchanged
        advanceTimeForElections();
        assertEquals(leaderId, assertOneLeader());
    }

    /**
     * Tests re-election mechanism after a leader is killed/disconnected.
     */
    @Test
    void testReElection() throws Exception {
        init(3);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);

        // wait a while and check if the leader has changed
        advanceTimeForElections();
        assertLeaderChanged(oldLeaderId, false /* oldLeader is not aware */);

        // rejoin old leader, old leader might try to recover its leadership due to its short update interval
        // but at the end there must still be one leader
        connect(oldLeaderId);
        advanceTimeForElections();
        assertOneLeader();
    }

    /**
     * Tests when the minority number of followers are killed, then the old leader still remains as the leader after
     * the dead followers are revived.
     */
    @Test
    void testLeaderNotChanged() throws Exception {
        int numServers = 5;
        init(numServers);
        int leaderId = assertOneLeader();

        kill((leaderId + 1) % numServers);
        kill((leaderId + 2) % numServers);

        advanceTimeForElections();
        assertLeaderNotChanged(leaderId);

        revive((leaderId + 1) % numServers);
        revive((leaderId + 2) % numServers);

        advanceTimeForElections();
        assertLeaderNotChanged(leaderId);
        assertOneLeader();
    }

    /**
     * Tests appending log entries when everything is running smoothly.
     */
    @Test
    void testWhenAllConnectedThenAppendSucceeds() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1", "cmd3");
        assertTrue(nodes[leaderId].append(expectedCommands.get(0)).get());
        assertTrue(nodes[leaderId].append(expectedCommands.get(1)).get());
        assertTrue(nodes[leaderId].append(expectedCommands.get(2)).get());
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests the failure of an append log entry event when the leader is killed/disconnected and there was no time
     * to elect a new one.
     */
    @Test
    void testWhenLeaderDisconnectedThenAppendFails() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        disconnect(leaderId);
        assertFalse(nodes[(leaderId + 1) % numServers].append("some-command").get());
    }

    /**
     * Tests if a command will be routed to the leader if the node is a follower.
     */
    @Test
    void testCommandRouting() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1", "cmd2");
        // if any of the following node is a follower, command will be routed to leader
        assertTrue(nodes[(leaderId + 1) % numServers].append(expectedCommands.get(0)).get());
        assertTrue(nodes[(leaderId + 2) % numServers].append(expectedCommands.get(1)).get());
        assertTrue(nodes[(leaderId + 3) % numServers].append(expectedCommands.get(2)).get());
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests cancellation of a command append event.
     */
    @Test
    void testAppendFutureCancellation() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        disconnect((leaderId + 1) % numServers);
        disconnect((leaderId + 2) % numServers);

        // break the quorum and try to append a command, progress cannot be made hence, we need to cancel the
        // future received.

        Future<Boolean> result0 = nodes[leaderId].append("cmd0");
        assertThrows(TimeoutException.class, () -> result0.get(1, TimeUnit.SECONDS));
        result0.cancel(true);
        assertThrows(CancellationException.class, result0::get);
    }

    /**
     * Tests if a follower node is down, the system is still operational.
     */
    @Test
    void testFollowerFailure() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        kill((leaderId + 1) % numServers);

        List<String> expectedCommands = Arrays.asList("cmd0");
        assertTrue(nodes[leaderId].append(expectedCommands.get(0)).get());

        revive((leaderId + 1) % numServers);
        advanceTimeForElections();  // allow sync time

        assertLeaderNotChanged(leaderId);
        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests when a leader fails, a new leader will be elected and the old leader will sync with all the changes
     * in the system state.
     */
    @Test
    void testLeaderFailure1() throws Exception {
        int numServers = 3;
        init(numServers);
        int oldLeaderId = assertOneLeader();

        kill(oldLeaderId);

        advanceTimeForElections(); // a new leader will be elected
        int newLeaderId = assertLeaderChanged(oldLeaderId, false);

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1");
        assertTrue(nodes[newLeaderId].append(expectedCommands.get(0)).get());

        revive(oldLeaderId);

        assertTrue(nodes[newLeaderId].append(expectedCommands.get(1)).get());
        // old leader will pick up all the changes during the above command update

        assertLeaderNotChanged(newLeaderId);
        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests when a leader fails, a new leader will be elected and the old leader will sync with all the changes
     * in the system state.
     */
    @Test
    void testLeaderFailure2() throws Exception {
        int numServers = 3;
        init(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);
        advanceTimeForElections(); // a new leader will be elected

        int newLeaderId = assertLeaderChanged(oldLeaderId, false);

        List<String> expectedCommands = Arrays.asList("cmd0");
        assertTrue(nodes[newLeaderId].append(expectedCommands.get(0)).get());

        connect(oldLeaderId);
        advanceTimeForElections(); // sync changes

        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests append event during a broken quorum and the leader is changed after the quorum is reestablished.
     * Append will fail.
     */
    @Test
    void testAppendWhenQuorumNotFormed1() throws Exception {
        int numServers = 5;
        init(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect((oldLeaderId + 1) % numServers);
        disconnect((oldLeaderId + 2) % numServers);
        disconnect((oldLeaderId + 3) % numServers);

        advanceTimeForElections();

        List<String> expectedCommands = Arrays.asList("cmd0");
        Future<Boolean> result0 = nodes[oldLeaderId].append(expectedCommands.get(0));
        assertThrows(TimeoutException.class, () -> result0.get(1, TimeUnit.SECONDS));

        kill(oldLeaderId);
        connect((oldLeaderId + 1) % numServers);
        connect((oldLeaderId + 2) % numServers);
        connect((oldLeaderId + 3) % numServers);
        advanceTimeForElections();

        revive(oldLeaderId);    // old leader will sync changes
        advanceTimeForElections();

        assertLeaderChanged(oldLeaderId, true /* oldLeader is aware */);

        // since a new leader will be elected, the old leader will discard the uncommitted command
        nodes[oldLeaderId].forceNotifyAll();
        assertFalse(result0.get());
        expectedCommands = new ArrayList<>();
        assertLogsEquals(expectedCommands);
    }

    /**
     * Tests append event during a broken quorum and the leader is not changed after the quorum is reestablished.
     * Append will succeed.
     */
//    @Test
    void testAppendWhenQuorumNotFormed2() throws Exception {
        int numServers = 5;
        init(numServers);
        int leaderId = assertOneLeader();

        kill((leaderId + 1) % numServers);
        kill((leaderId + 2) % numServers);
        kill((leaderId + 3) % numServers);

        advanceTimeForElections();

        List<String> expectedCommands = Arrays.asList("cmd0");
        Future<Boolean> result0 = nodes[leaderId].append(expectedCommands.get(0));
        assertThrows(TimeoutException.class, () -> result0.get(1, TimeUnit.SECONDS));

        // new leader won't be elected
        advanceTimeForElections();

        revive((leaderId + 1) % numServers);
        revive((leaderId + 2) % numServers);
        revive((leaderId + 3) % numServers);

        advanceTimeForElections();
        assertLeaderNotChanged(leaderId);   // fixme: doesn't work properly due to leader being changed after revival of killed nodes. This is normal but makes test unusable.

        assertTrue(result0.get());  // the cmd should be applied and synced.
        assertLogsEquals(expectedCommands);
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
        init(numServers);
        int oldLeaderId = assertOneLeader();

        disconnect(oldLeaderId);
        disconnect((oldLeaderId + 2) % numServers);
        disconnect((oldLeaderId + 3) % numServers);

        advanceTimeForElections();

        List<String> expectedCommands = Arrays.asList("cmd0", "cmd1");
        Future<Boolean> future0 = nodes[oldLeaderId].append(expectedCommands.get(0));
        assertThrows(TimeoutException.class, () -> future0.get(1, TimeUnit.SECONDS));

        connect((oldLeaderId + 2) % numServers);
        connect((oldLeaderId + 3) % numServers);

        advanceTimeForElections();  // the "cmd0" entry will not be applied at all
        int newLeaderId = assertLeaderChanged(oldLeaderId, false /* oldLeader is not aware */);
        Future<Boolean> future1 = nodes[newLeaderId].append(expectedCommands.get(1));
        assertTrue(future1.get()); // append a new entry to log. oldLeader's entry will not be synced.

        connect(oldLeaderId);  // connect old leader and discover new one
        advanceTimeForElections();
        assertLeaderNotChanged(newLeaderId);    // oldLeader should not be able to become the leader at this point.
        assertFalse(future0.get()); // this entry is long gone
        expectedCommands = Arrays.asList("cmd1");

        assertLogsEquals(expectedCommands); // log item will be applied as soon as the quorum is formed again.
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
        init(numServers);
        int leaderId = assertOneLeader();

        disconnect((leaderId + 1) % numServers);
        disconnect((leaderId + 2) % numServers);
        List<String> expectedCommands = Arrays.asList("cmd0");
        nodes[leaderId].append(expectedCommands.get(0)).get();

        advanceTimeForElections();
        connect((leaderId + 1) % numServers);
        connect((leaderId + 2) % numServers);
        advanceTimeForElections();

        assertOneLeader();  // leader might've changed but there must still be only one leader.
        assertLogsEquals(expectedCommands);
    }
}
