package com.mboysan.consensus;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class BizurNodeTest extends BizurTestBase {

    @Test
    void testWhenServerNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        BizurNode node = new BizurNode(0, transport);

        assertThrows(IllegalStateException.class, () -> node.get(new KVGetRequest("some-key")));

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

        kill(oldLeaderId);

        // wait a while and check if the leader has changed
        advanceTimeForElections();
        int newLeaderId = assertLeaderChanged(oldLeaderId, false /* oldLeader is not aware */);

        // rejoin old leader, old leader might try to recover its leadership due to its short update interval
        // but at the end there must still be one leader
        revive(oldLeaderId);
        advanceTimeForElections();
        assertLeaderOfMajority(newLeaderId);
    }

    /**
     * Tests validation failures for operations when key/value provided are null.
     */
    @Test
    void testWhenInvalidKeyValueOperationFails() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        assertThrows(ExecutionException.class, () -> nodes[leaderId].set("key", null).get());
        assertThrows(ExecutionException.class, () -> nodes[leaderId].set(null, "value").get());
        assertThrows(ExecutionException.class, () -> nodes[leaderId].set(null, null).get());
        assertThrows(ExecutionException.class, () -> nodes[leaderId].get((String) null).get());
    }

    /**
     * Tests get operation still succeeds (but returns null value) if we request a key that was not inserted before.
     */
    @Test
    void testWhenNonExistingKeyThenGetReturnsNull() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        assertNull(nodes[leaderId].get("some-non-existing-key").get());
    }

    /**
     * Tests set/get operations when everything is running smoothly.
     */
    @Test
    void testWhenAllConnectedThenSetAndGetSucceeds() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        Map<String, String> expectedKVs = new HashMap<>() {
            {put("k0", "v0");}
            {put("k1", "v1");}
            {put("k2", "v2");}
        };

        for (String expKey : expectedKVs.keySet()) {
            String expVal = expectedKVs.get(expKey);
            nodes[leaderId].set(expKey, expVal).get();
            assertEquals(expVal, nodes[leaderId].get(expKey).get());
        }

        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);
    }

    /**
     * Tests if a command will be routed to the leader if the node is not the leader.
     */
    @Test
    void testCommandRouting() throws Exception {
        int numServers = 3;
        init(numServers);

        int leaderId = assertOneLeader();

        Map<String, String> expectedKVs = new HashMap<>() {
            {put("k0", "v0");}
            {put("k1", "v1");}
            {put("k2", "v2");}
        };

        // non-leader nodes will route the command to the leader
        nodes[(leaderId + 1) % numServers].set("k0", "v0").get();
        nodes[(leaderId + 2) % numServers].set("k1", "v1").get();
        nodes[(leaderId + 3) % numServers].set("k2", "v2").get();
        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);
    }

    /**
     * Tests cancellation of an operation.
     */
    @Test
    void testOperationFutureCancellation() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        Future<Void> result0 = nodes[leaderId].set("key", "val");
        result0.cancel(true);
        assertThrows(CancellationException.class, result0::get);
    }

    /**
     * Tests the failure of set/get operations when the leader is killed/disconnected and there was no time
     * to elect a new one.
     */
    @Test
    void testWhenLeaderKilledThenSetAndGetFails() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        kill(leaderId);

        assertThrows(ExecutionException.class, () -> nodes[leaderId].set("testKey", "testVal").get());
        assertThrows(ExecutionException.class, () -> nodes[leaderId].get("testKey").get());
    }

    /**
     * Tests the failure of set/get operations on any other node other than the leader when the leader is
     * killed/disconnected and there was no time to elect a new one.
     */
    @Test
    void testWhenLeaderKilledThenSetFailsOnOtherNode() throws Exception {
        int numServers = 3;
        init(numServers);
        int leaderId = assertOneLeader();

        kill(leaderId);

        assertThrows(ExecutionException.class, () -> nodes[(leaderId + 1) % numServers].set("testKey", "testVal").get());
        assertThrows(ExecutionException.class, () -> nodes[(leaderId + 2) % numServers].get("testKey").get());
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

        Map<String, String> expectedKVs = new HashMap<>() {
            {put("k0", "v0");}
            {put("k1", "v1");}
        };
        nodes[leaderId].set("k0", "v0").get();

        revive((leaderId + 1) % numServers);
        // sync: Bizur triggers bucket sync as soon as a set operation is performed by the leader.
        nodes[leaderId].set("k1", "v1").get();

        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);
    }
}
