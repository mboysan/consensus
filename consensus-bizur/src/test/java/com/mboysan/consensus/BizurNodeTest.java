package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.message.KVDeleteRequest;
import com.mboysan.consensus.message.KVDeleteResponse;
import com.mboysan.consensus.message.KVGetRequest;
import com.mboysan.consensus.message.KVGetResponse;
import com.mboysan.consensus.message.KVIterateKeysRequest;
import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.message.KVOperationResponse;
import com.mboysan.consensus.message.KVSetRequest;
import com.mboysan.consensus.message.KVSetResponse;
import com.mboysan.consensus.message.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BizurNodeTest extends NodeTestBase {

    private boolean skipTeardown;
    private BizurNode[] nodes;
    private InVMTransport transport;

    void initCluster(int numNodes, int numBuckets) throws IOException, ExecutionException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        nodes = new BizurNode[numNodes];
        transport = new InVMTransport();
        for (int i = 0; i < numNodes; i++) {
            BizurConfig bizurConfig = bizurConfig(i, numNodes, numBuckets);
            BizurNode node = new BizurNode(bizurConfig, transport);
            nodes[i] = node;

            futures.add(node.start());
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        // all nodes must agree on the leaders of all the bucket ranges.
        assertAllNodesAgreedOnRangeLeaders();
    }

    private BizurConfig bizurConfig(int nodeId, int numPeers, int numBuckets) {
        Properties properties = new Properties();
        properties.put("node.id", nodeId + "");
        properties.put("bizur.numPeers", numPeers + "");
        properties.put("bizur.numBuckets", numBuckets + "");
        properties.put("bizur.updateIntervalMs", 50 * (nodeId + 1) + "");
        return Configuration.newInstance(BizurConfig.class, properties);
    }

    @Override
    InVMTransport getTransport() {
        return transport;
    }

    @Override
    BizurNode getNode(int nodeId) {
        return nodes[nodeId];
    }

    @Test
    void testWhenNodeNotReadyThenThrowsException() {
        Transport transport = new InVMTransport();
        BizurNode node = new BizurNode(bizurConfig(0, 3, 1), transport);

        KVGetRequest request = new KVGetRequest("some-key");
        assertThrows(IllegalStateException.class, () -> node.get(request));
        skipTeardown = true;
    }

    @Test
    void testReElectionWithSingleBucket() throws Exception {
        testReElection(3, 1);
    }

    @Test
    void testReElectionWithMultiBucket() throws Exception {
        testReElection(3, 100);
    }

    private void testReElection(int numNodes, int numBuckets) throws Exception {
        initCluster(numNodes, numBuckets);
        assertAllNodesAgreedOnRangeLeaders();

        // assuming numNodes=3 and numBuckets=7:
        // bucket-0,3,6 is in range-0, and supposed leader of that range is node-0
        kill(0);
        // after killing node-0, leader of the bucket range-0 will change, which will be agreed by majority.
        assertMajorityAgreedOnRangeLeaders();

        revive(0);
        // when node-0 is revived, this node will reclaim its leadership of range-0.
        assertAllNodesAgreedOnRangeLeaders();
    }

    /**
     * Tests get operation still succeeds (but returns null value) if we request a key that was not inserted before.
     */
    @Test
    void testWhenNonExistingKeyThenGetReturnsNull() throws Exception {
        initCluster(3, 1);
        assertNull(get(0, "some-non-existing-key"));
    }

    @Test
    void testApiSimpleWithSingleBucket() throws Exception {
        testApiSimple(3, 1);
    }

    @Test
    void testApiSimpleWithMultiBucket() throws Exception {
        testApiSimple(3, 7);
    }

    /**
     * Tests set/get operations when everything is running smoothly.
     */
    private void testApiSimple(int numNodes, int numBuckets) throws Exception {
        initCluster(numNodes, numBuckets);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
            put("k2", "v2");
            put("k3", "v3");
        }};

        for (String expKey : expectedKVs.keySet()) {
            String expVal = expectedKVs.get(expKey);
            set(0, expKey, expVal);
            assertEquals(expVal, get(1, expKey));
        }
        delete(2, "k3");
        expectedKVs.remove("k3");

        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
    }

    /**
     * Tests k/v operations when leader is killed.
     */
    @Test
    void testKVOpsOnLeaderFailure() throws Exception {
        initCluster(3, 1);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};

        // since we have only 1 bucket, all key-values will be written on bucket-0 which is on range-0,
        // for which node-0 will be elected as its leader.
        kill(0);
        assertThrows(IllegalStateException.class, () -> set(0, "k0", "v0"));
        awaiting(() -> set(1, "k0", "v0"));

        revive(0);
        set(0, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
    }

    /**
     * Tests k/v operations when follower is killed.
     */
    @Test
    void testKVOpsOnFollowerFailure() throws Exception {
        initCluster(3, 1);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};

        // since we have only 1 bucket, all key-values will be written on bucket-0 which is on range-0,
        // for which node-0 will be elected as its leader.
        kill(1);    // follower of range-0
        assertThrows(IllegalStateException.class, () -> set(1, "k0", "v0"));
        set(0, "k0", "v0");

        revive(1);
        set(1, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
    }

    /**
     * Tests k/v operations when leader is disconnected from the network, i.e. it will still continue to
     * run on the background.
     */
    @Test
    void testKVOpsOnLeaderDisconnected() throws Exception {
        initCluster(3, 1);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};

        // since we have only 1 bucket, all key-values will be written on bucket-0 which is on range-0,
        // for which node-0 will be elected as its leader.
        disconnect(0);
        assertThrows(BizurException.class, () -> set(0, "k0", "v0"));
        awaiting(() -> set(1, "k0", "v0"));

        connect(0);
        awaiting(() -> set(0, "k1", "v1"));
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
    }

    /**
     * Tests k/v operations when follower is disconnected from the network, i.e. it will still continue to
     * run on the background but leader cannot ask for confirmations on consensus messaging.
     */
    @Test
    void testKVOpsOnFollowerDisconnected() throws Exception {
        initCluster(3, 1);

        Map<String, String> expectedKVs = new HashMap<>() {{
            put("k0", "v0");
            put("k1", "v1");
        }};

        // since we have only 1 bucket, all key-values will be written on bucket-0 which is on range-0,
        // for which node-0 will be elected as its leader.
        disconnect(1);  // follower of range-0
        assertThrows(IOException.class, () -> set(1, "k0", "v0"));
        set(0, "k0", "v0");

        // at this point, if enough time passed, node-1 will try to assume leadership and since it's disconnected
        // from the network its leader id for range-0 will be -1.
        connect(1);
        // Since it might have already lost the active leader, it might throw exceptions.
        awaiting(() -> set(1, "k1", "v1"));
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
    }

    // ------------------------------------------------------------- assertions

    /**
     * asserts that all ranges in all nodes have the supposed leader.
     */
    private void assertAllNodesAgreedOnRangeLeaders() {
        for (BizurNode node : nodes) {
            for (BucketRange range : node.getBucketRanges().values()) {
                assertRangeLeaderIsSupposedLeaderAtNode(node.getNodeId(), range.getRangeIndex());
            }
        }
    }

    /**
     * asserts that a given range has the supposed leader assigned at the node being checked.
     */
    private void assertRangeLeaderIsSupposedLeaderAtNode(int nodeId, int rangeIndex) {
        awaiting(() -> {
            int supposedLeader = nodes[nodeId].nodeIdForRangeIndex(rangeIndex);
            assertEquals(supposedLeader, nodes[nodeId].getBucketRange(rangeIndex).getLeaderId());
        });
    }

    /**
     * asserts that all bucket ranges have the same leaders in the majority of the nodes.
     */
    private void assertMajorityAgreedOnRangeLeaders() {
        for (BizurNode node : nodes) {
            for (BucketRange range : node.getBucketRanges().values()) {
                assertMajorityAgreedOnLeaderOfRange(range.getRangeIndex());
            }
        }
    }

    /**
     * asserts that for the majority of the nodes, the leader is chosen as the leader of the given range.
     */
    private void assertMajorityAgreedOnLeaderOfRange(int rangeIndex) {
        awaiting(() -> {
            int discrepancyCount = 0;
            int majorityLeader = -1;
            for (BizurNode node : nodes) {
                int leaderId = node.getBucketRange(rangeIndex).getLeaderId();
                if (majorityLeader == -1) {
                    majorityLeader = leaderId;
                }
                if (leaderId == -1 || majorityLeader != leaderId) {
                    discrepancyCount++;
                }
            }
            assertTrue(discrepancyCount < ((nodes.length / 2) + 1), "range leader is not determined, discrepancyCount=" + discrepancyCount);
            assertTrue(nodes[majorityLeader].isRunning(), "node-" + majorityLeader + " is believed to be the leader of the majority but it's not alive");
        });
    }

    private void assertKeyValueIntegrity(Map<String, String> expectedKVMap) throws Exception {
        for (BizurNode node : nodes) {
            Set<String> actualKeys = iterateKeys(node.getNodeId());
            assertEquals(expectedKVMap.size(), actualKeys.size());
            for (String key : actualKeys) {
                assertEquals(expectedKVMap.get(key), get(node.getNodeId(), key));
            }
        }
    }

    // ------------------------------------------------------------- node operations

    private String get(int byNodeId, String key) throws Exception {
        KVGetRequest request = new KVGetRequest(key);
        KVGetResponse response = nodes[byNodeId].get(request);
        validateResponse(response, request);
        return response.getValue();
    }

    private void set(int byNodeId, String key, String value) throws Exception {
        KVSetRequest request = new KVSetRequest(key, value);
        KVSetResponse response = nodes[byNodeId].set(request);
        validateResponse(response, request);
    }

    public void delete(int byNodeId, String key) throws Exception {
        KVDeleteRequest request = new KVDeleteRequest(key);
        KVDeleteResponse response = nodes[byNodeId].delete(request);
        validateResponse(response, request);
    }

    public Set<String> iterateKeys(int byNodeId) throws Exception {
        KVIterateKeysRequest request = new KVIterateKeysRequest();
        KVIterateKeysResponse response = nodes[byNodeId].iterateKeys(request);
        validateResponse(response, request);
        return response.getKeys();
    }

    private void validateResponse(KVOperationResponse response, Message forRequest) throws BizurException {
        if (!response.isSuccess()) {
            throw new BizurException("failed response=[%s] for request=[%s]".formatted(response, forRequest));
        }
    }

    @AfterEach
    void tearDown() {
        if (skipTeardown) {
            return;
        }
        assertNotNull(transport);
        assertNotNull(nodes);
        Arrays.stream(nodes).forEach(AbstractNode::shutdown);
        transport.shutdown();
    }
}
