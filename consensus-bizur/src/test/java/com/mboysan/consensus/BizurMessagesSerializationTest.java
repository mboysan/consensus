package com.mboysan.consensus;

import com.mboysan.consensus.message.*;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static com.mboysan.consensus.util.SerializationTestUtil.assertSerialized;
import static com.mboysan.consensus.util.SerializationTestUtil.serializeDeserialize;

class BizurMessagesSerializationTest {

    @Test
    void testSerializeHeartbeatMessages() throws Exception {
        HeartbeatRequest reqExpected = new HeartbeatRequest(1234L);
        HeartbeatRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        HeartbeatResponse respExpected = new HeartbeatResponse(4321L);
        HeartbeatResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeKVDeleteMessages() throws Exception {
        KVDeleteRequest reqExpected = new KVDeleteRequest("abc");
        KVDeleteRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        KVDeleteResponse respExpected = new KVDeleteResponse(false, new BizurException("err"));
        KVDeleteResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeKVGetMessages() throws Exception {
        KVGetRequest reqExpected = new KVGetRequest("abc");
        KVGetRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        KVGetResponse respExpected = new KVGetResponse(false, new BizurException("err"), "N/A");
        KVGetResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeKVIterateKeysMessages() throws Exception {
        KVIterateKeysRequest reqExpected = new KVIterateKeysRequest();
        KVIterateKeysRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        KVIterateKeysResponse respExpected = new KVIterateKeysResponse(
                false, new BizurException("err"), getTestKeySet());
        KVIterateKeysResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeKVSetMessages() throws Exception {
        KVSetRequest reqExpected = new KVSetRequest("k0", "v0");
        KVSetRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        KVSetResponse respExpected = new KVSetResponse(false, new BizurException("err"));
        KVSetResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializePleaseVoteMessages() throws Exception {
        PleaseVoteRequest reqExpected = new PleaseVoteRequest(3, 1);
        PleaseVoteRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        PleaseVoteResponse respExpected = new PleaseVoteResponse(true);
        PleaseVoteResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeReplicaReadMessages() throws Exception {
        ReplicaReadRequest reqExpected = new ReplicaReadRequest(1, 3);
        ReplicaReadRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        ReplicaReadResponse respExpected = new ReplicaReadResponse(true, createTestBucket());
        ReplicaReadResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    @Test
    void testSerializeReplicaWriteMessages() throws Exception {
        ReplicaWriteRequest reqExpected = new ReplicaWriteRequest(1, createTestBucket());
        ReplicaWriteRequest reqActual = serializeDeserialize(reqExpected);
        assertSerialized(reqExpected, reqActual);

        ReplicaWriteResponse respExpected = new ReplicaWriteResponse(true);
        ReplicaWriteResponse respActual = serializeDeserialize(respExpected);
        assertSerialized(respExpected, respActual);
    }

    private Set<String> getTestKeySet() {
        /* If we do the following, serialization fails, maybe due to java creating an anon inner class?
        Set<String> keys = new LinkedHashSet<>() {{
            add("k0"); add("k1");
        }};
        */
        Set<String> keys = new LinkedHashSet<>();
        keys.add("k0");
        keys.add("k1");
        keys.add("k2");
        return keys;
    }

    private Bucket createTestBucket() {
        Bucket testBucket = new Bucket(1);
        testBucket.putOp("k0", "v0");
        testBucket.putOp("k1", "v1");
        testBucket.putOp("k2", "v2");
        testBucket.setVerElectId(2);
        testBucket.setVerCounter(3);
        return testBucket;
    }
}
