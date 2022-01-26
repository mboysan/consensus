package com.mboysan.consensus;

import com.mboysan.consensus.message.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BizurNodeTest extends NodeTestBase<BizurNode> implements BizurInternals {

    @Test
    void testWhenServerNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        BizurNode node = createNode(0, transport, null);

        KVGetRequest request = new KVGetRequest("some-key");
        assertThrows(IllegalStateException.class, () -> node.get(request));

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

        assertThrows(BizurException.class, () -> set(leaderId, "key", null));
        assertThrows(BizurException.class, () -> set(leaderId, null, "value"));
        assertThrows(BizurException.class, () -> set(leaderId, null, null));
        assertThrows(BizurException.class, () -> get(leaderId, null));
    }

    /**
     * Tests get operation still succeeds (but returns null value) if we request a key that was not inserted before.
     */
    @Test
    void testWhenNonExistingKeyThenGetReturnsNull() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        assertNull(get(leaderId, "some-non-existing-key"));
    }

    /**
     * Tests set/get operations when everything is running smoothly.
     */
    @Test
    void testWhenAllConnectedThenSetGetAndDeleteSucceeds() throws Exception {
        init(3);
        int leaderId = assertOneLeader();

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
            put("k2", "v2");
            put("k3", "v3");
        }};

        for (String expKey : expectedKVs.keySet()) {
            String expVal = expectedKVs.get(expKey);
            set(leaderId, expKey, expVal);
            assertEquals(expVal, get(leaderId, expKey));
        }
        delete(leaderId, "k3");
        expectedKVs.remove("k3");

        assertLeaderNotChanged(leaderId);
        assertBucketIntegrity(expectedKVs);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertBucketIntegrity(expectedKVs);
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
        set((leaderId + 1) % numServers, "k0", "v0");
        set((leaderId + 2) % numServers, "k1", "v1");
        set((leaderId + 3) % numServers, "k2", "v2");
        assertLeaderNotChanged(leaderId);
        assertBucketIntegrity(expectedKVs);

        advanceTimeForElections();  // everything should stay the same even after some time passes
        assertLeaderNotChanged(leaderId);
        assertBucketIntegrity(expectedKVs);
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

        assertThrows(Exception.class, () -> set(leaderId, "testKey", "testVal"));
        assertThrows(Exception.class, () -> get(leaderId, "testKey"));
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

        assertThrows(IOException.class, () -> set((leaderId + 1) % numServers, "testKey", "testVal"));
        assertThrows(IOException.class, () -> get((leaderId + 2) % numServers, "testKey"));
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
        set(leaderId, "k0", "v0");

        revive((leaderId + 1) % numServers);
        // sync: Bizur triggers bucket sync as soon as a set operation is performed by the leader.
        set(leaderId, "k1", "v1");

        assertLeaderNotChanged(leaderId);
        assertBucketIntegrity(expectedKVs);
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

        set(newLeaderId, "k0", "v0");

        advanceTimeForElections();
        assertLeaderOfMajority(newLeaderId);    // new leader is still the leader of majority

        revive(oldLeaderId);

        assertThrows(BizurException.class, () -> set(oldLeaderId, "some-key", "some-value"));
        // at this point, the old leader will understand that it's no longer the leader.

        set(newLeaderId, "k1", "v1");
        // Bizur propagates the leader changes after a write operation. Therefore, the old leader will update its vote
        // for new leader after this operation.

        assertOneLeader();
        advanceTimeForElections();  // let some time pass
        assertOneLeader();

        assertBucketIntegrity(expectedKVs);
    }

    // --------------------------------------------------------------------------------
    // Assertions & convenient methods for BizurNode operations
    // --------------------------------------------------------------------------------

    private void assertBucketIntegrity(BizurNode node, Map<String, String> expectedBucketMap) throws Exception {
        int totalSize = 0;
        for (Integer bucketIndex : node.getBucketMap().keySet()) {
            Bucket bucket = node.getBucketMap().get(bucketIndex);
            for (String key : bucket.getKeySetOp()) {
                assertEquals(expectedBucketMap.get(key), bucket.getOp(key));
                totalSize++;
            }
        }
        assertEquals(expectedBucketMap.size(), totalSize);

        Set<String> keys = iterateKeys(node.getNodeId());
        assertEquals(expectedBucketMap.size(), keys.size());
    }

    private void assertBucketIntegrity(Map<String, String> expectedBucketMap) throws Exception {
        for (BizurNode node : getNodes()) {
            assertBucketIntegrity(node, expectedBucketMap);
        }
    }

    private String get(int byNodeId, String key) throws Exception {
        KVGetRequest request = new KVGetRequest(key);
        KVGetResponse response = getNode(byNodeId).get(request);
        validateResponse(response, request);
        return response.getValue();
    }

    private void set(int byNodeId, String key, String value) throws Exception {
        KVSetRequest request = new KVSetRequest(key, value);
        KVSetResponse response = getNode(byNodeId).set(request);
        validateResponse(response, request);
    }

    public void delete(int byNodeId, String key) throws Exception {
        KVDeleteRequest request = new KVDeleteRequest(key);
        KVDeleteResponse response = getNode(byNodeId).delete(request);
        validateResponse(response, request);
    }

    public Set<String> iterateKeys(int byNodeId) throws Exception {
        KVIterateKeysRequest request = new KVIterateKeysRequest();
        KVIterateKeysResponse response = getNode(byNodeId).iterateKeys(request);
        validateResponse(response, request);
        return response.getKeys();
    }

    private void validateResponse(KVOperationResponse response, Message forRequest) throws BizurException {
        if (!response.isSuccess()) {
            throw new BizurException("failed response=[%s] for request=[%s]".formatted(response, forRequest));
        }
    }
}
