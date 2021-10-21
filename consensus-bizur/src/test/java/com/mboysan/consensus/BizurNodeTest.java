package com.mboysan.consensus;

import com.mboysan.consensus.message.KVGetRequest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BizurNodeTest extends NodeTestBase<BizurNode> implements BizurInternals {

    @Test
    void testWhenServerNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        BizurNode node = createNode(0, transport, null);

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

        assertThrows(ExecutionException.class, () -> getNode(leaderId).set("key", null).get());
        assertThrows(ExecutionException.class, () -> getNode(leaderId).set(null, "value").get());
        assertThrows(ExecutionException.class, () -> getNode(leaderId).set(null, null).get());
        assertThrows(ExecutionException.class, () -> getNode(leaderId).get((String) null).get());
    }

    /**
     * Tests get operation still succeeds (but returns null value) if we request a key that was not inserted before.
     */
    @Test
    void testWhenNonExistingKeyThenGetReturnsNull() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        assertNull(getNode(leaderId).get("some-non-existing-key").get());
    }

    /**
     * Tests set/get operations when everything is running smoothly.
     */
    @Test
    void testWhenAllConnectedThenSetAndGetSucceeds() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
            put("k2", "v2");
        }};

        for (String expKey : expectedKVs.keySet()) {
            String expVal = expectedKVs.get(expKey);
            getNode(leaderId).set(expKey, expVal).get();
            assertEquals(expVal, getNode(leaderId).get(expKey).get());
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

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
            put("k2", "v2");
        }};

        // non-leader nodes will route the command to the leader
        getNode((leaderId + 1) % numServers).set("k0", "v0").get();
        getNode((leaderId + 2) % numServers).set("k1", "v1").get();
        getNode((leaderId + 3) % numServers).set("k2", "v2").get();
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

        Future<Void> result0 = getNode(leaderId).set("key", "val");
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

        assertThrows(ExecutionException.class, () -> getNode(leaderId).set("testKey", "testVal").get());
        assertThrows(ExecutionException.class, () -> getNode(leaderId).get("testKey").get());
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

        assertThrows(ExecutionException.class, () -> getNode((leaderId + 1) % numServers).set("testKey", "testVal").get());
        assertThrows(ExecutionException.class, () -> getNode((leaderId + 2) % numServers).get("testKey").get());
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

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};
        getNode(leaderId).set("k0", "v0").get();

        revive((leaderId + 1) % numServers);
        // sync: Bizur triggers bucket sync as soon as a set operation is performed by the leader.
        getNode(leaderId).set("k1", "v1").get();

        assertLeaderNotChanged(leaderId);
        assertBucketMapsEquals(expectedKVs);
    }

    @Test
    void testLeaderChangedAndOldLeaderSyncs() throws Exception {
        init(3);
        int oldLeaderId = assertOneLeader();

        kill(oldLeaderId);

        // wait a while and check if the leader has changed
        advanceTimeForElections();
        int newLeaderId = assertLeaderChanged(oldLeaderId, false /* oldLeader is not aware */);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};

        getNode(newLeaderId).set("k0", "v0").get();

        advanceTimeForElections();
        assertLeaderOfMajority(newLeaderId);    // new leader is still the leader of majority

        revive(oldLeaderId);

        assertThrows(ExecutionException.class, () -> getNode(oldLeaderId).set("some-key", "some-value").get());
        // at this point, the old leader will understand that it's no longer the leader.

        getNode(newLeaderId).set("k1", "v1").get();
        // Bizur propagates the leader changes after a write operation. Therefore, the old leader will update its vote
        // for new leader after this operation.

        assertOneLeader();
        advanceTimeForElections();  // let some time pass
        assertOneLeader();

        assertBucketMapsEquals(expectedKVs);
    }

    private void assertBucketMapsEquals(BizurNode node, Map<String, String> expectedBucketMap) {
        int totalSize = 0;
        for (Integer bucketIndex : node.getBucketMap().keySet()) {
            Bucket bucket = node.getBucketMap().get(bucketIndex);
            for (String key : bucket.getKeySetOp()) {
                assertEquals(expectedBucketMap.get(key), bucket.getOp(key));
                totalSize++;
            }
        }
        assertEquals(expectedBucketMap.size(), totalSize);
    }

    private void assertBucketMapsEquals(Map<String, String> expectedBucketMap) {
        for (BizurNode node : getNodes()) {
            assertBucketMapsEquals(node, expectedBucketMap);
        }
    }
}
