package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.message.BizurKVDeleteRequest;
import com.mboysan.consensus.message.BizurKVDeleteResponse;
import com.mboysan.consensus.message.BizurKVGetRequest;
import com.mboysan.consensus.message.BizurKVGetResponse;
import com.mboysan.consensus.message.BizurKVIterateKeysRequest;
import com.mboysan.consensus.message.BizurKVIterateKeysResponse;
import com.mboysan.consensus.message.BizurKVOperationResponse;
import com.mboysan.consensus.message.BizurKVSetRequest;
import com.mboysan.consensus.message.BizurKVSetResponse;
import com.mboysan.consensus.message.CheckBizurIntegrityRequest;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BizurNodeTest extends NodeTestBase {

    private boolean skipTeardown;
    private BizurNode[] nodes;
    private InVMTransport transport;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

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
        properties.put(NodeConfig.Param.NODE_ID, nodeId + "");
        properties.put(BizurConfig.Param.NUM_PEERS, numPeers + "");
        properties.put(BizurConfig.Param.NUM_BUCKETS, numBuckets + "");
        properties.put(BizurConfig.Param.UPDATE_INTERVAL_MS, 50 + "");
        return CoreConfig.newInstance(BizurConfig.class, properties);
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

        BizurKVGetRequest request = new BizurKVGetRequest("some-key");
        assertThrows(IllegalStateException.class, () -> node.get(request));
        skipTeardown = true;
    }

    @Test
    void testIntegrityCheckFailsWhenMajorityCannotRespond() throws Exception {
        int numServers = 3;
        initCluster(numServers, 1);
        int nodeIdToCheck = 0;

        disconnect((nodeIdToCheck + 1) % numServers);
        disconnect((nodeIdToCheck + 2) % numServers);

        boolean success = checkIntegrity(nodeIdToCheck);
        assertFalse(success);
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
        assertIntegrityCheckPassed();
    }

    /**
     * Tests get operation still succeeds (but returns null value) if we request a key that was not inserted before.
     */
    @Test
    void testWhenNonExistingKeyThenGetReturnsNull() throws Exception {
        initCluster(3, 1);
        assertNull(getAwaiting(0, "some-non-existing-key"));
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
            setAwaiting(0, expKey, expVal);
            assertEquals(expVal, getAwaiting(1, expKey));
        }
        deleteAwaiting(2, "k3");
        expectedKVs.remove("k3");

        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
        assertIntegrityCheckPassed();
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
        setAwaiting(1, "k0", "v0");

        revive(0);
        setAwaiting(0, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
        assertIntegrityCheckPassed();
    }

    /**
     * A second k/v operations test to assert key/value integrity when leader is killed. This is a known scenario that
     * was failing during integration tests, so we ensure this test succeeds in unit tests as well.
     */
    @Test
    void testKVOpsOnLeaderFailure2() throws Exception {
        initCluster(5, 5);

        // key 'a' belongs to bucket-2 which belongs to node-2.
        final String expectedKey = "a";
        final String expectedValue = "v0";

        setAwaiting(0, expectedKey, expectedValue);

        kill(2);

        String actualValue = getAwaiting(0, expectedKey);
        assertEquals(expectedValue, actualValue);
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
        setAwaiting(0, "k0", "v0");

        revive(1);
        setAwaiting(1, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
        assertIntegrityCheckPassed();
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
        setAwaiting(1, "k0", "v0");

        connect(0);
        setAwaiting(0, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
        assertIntegrityCheckPassed();
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
        setAwaiting(0, "k0", "v0");

        // at this point, if enough time passed, node-1 will try to assume leadership and since it's disconnected
        // from the network its leader id for range-0 will be -1.
        connect(1);
        // Since it might have already lost the active leader, it might throw exceptions.
        setAwaiting(1, "k1", "v1");
        assertKeyValueIntegrity(expectedKVs);

        assertAllNodesAgreedOnRangeLeaders();
        assertIntegrityCheckPassed();
    }

    // ------------------------------------------------------------- assertions

    private void assertIntegrityCheckPassed() throws IOException {
        for (BizurNode node : nodes) {
            assertTrue(checkIntegrity(node.getNodeId()));
        }
    }

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

    private void assertKeyValueIntegrity(Map<String, String> expectedKVMap) {
        for (BizurNode node : nodes) {
            Set<String> actualKeys = iterateKeysAwaiting(node.getNodeId());
            assertEquals(expectedKVMap.size(), actualKeys.size());
            for (String key : actualKeys) {
                assertEquals(expectedKVMap.get(key), getAwaiting(node.getNodeId(), key));
            }
        }
    }

    // ------------------------------------------------------------- node operations

    private String getAwaiting(int byNodeId, String key) {
        return awaiting(() -> get(byNodeId, key));
    }

    private void setAwaiting(int byNodeId, String key, String value) {
        awaiting(() -> set(byNodeId, key, value));
    }

    private void deleteAwaiting(int byNodeId, String key) {
        awaiting(() -> delete(byNodeId, key));
    }

    private Set<String> iterateKeysAwaiting(int byNodeId) {
        return awaiting(() -> iterateKeys(byNodeId));
    }

    private String get(int byNodeId, String key) throws Exception {
        BizurKVGetRequest request = new BizurKVGetRequest(key);
        BizurKVGetResponse response = nodes[byNodeId].get(request);
        validateResponse(response, request);
        return response.getValue();
    }

    private void set(int byNodeId, String key, String value) throws Exception {
        BizurKVSetRequest request = new BizurKVSetRequest(key, value);
        BizurKVSetResponse response = nodes[byNodeId].set(request);
        validateResponse(response, request);
    }

    private void delete(int byNodeId, String key) throws Exception {
        BizurKVDeleteRequest request = new BizurKVDeleteRequest(key);
        BizurKVDeleteResponse response = nodes[byNodeId].delete(request);
        validateResponse(response, request);
    }

    private Set<String> iterateKeys(int byNodeId) throws Exception {
        BizurKVIterateKeysRequest request = new BizurKVIterateKeysRequest();
        BizurKVIterateKeysResponse response = nodes[byNodeId].iterateKeys(request);
        validateResponse(response, request);
        return response.getKeys();
    }

    private void validateResponse(BizurKVOperationResponse response, Message forRequest) throws BizurException {
        if (!response.isSuccess()) {
            throw new BizurException("failed response=[%s] for request=[%s]".formatted(response, forRequest));
        }
    }

    private boolean checkIntegrity(int nodeId) throws IOException {
        return nodes[nodeId].checkBizurIntegrity(
                new CheckBizurIntegrityRequest(CheckBizurIntegrityRequest.Level.STATE_FROM_ALL)).isSuccess();
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
