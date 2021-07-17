package com.mboysan.consensus;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mboysan.consensus.util.SerializationTestUtil.assertSerialized;
import static com.mboysan.consensus.util.SerializationTestUtil.serializeDeserialize;

public class RaftMessagesSerializationTest {

    @Test
    void testSerializeAppendEntriesRequest() throws Exception {
        List<LogEntry> entries = Arrays.asList(new LogEntry("cmd0", 1), new LogEntry("cmd1", 2));
        AppendEntriesRequest expected = new AppendEntriesRequest(1, 2, 3, 4, entries, 5);
        AppendEntriesRequest actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

    @Test
    void testSerializeAppendEntriesResponse() throws Exception {
        AppendEntriesResponse expected = new AppendEntriesResponse(1, true, 2);
        AppendEntriesResponse actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

    @Test
    void testSerializeRequestVoteRequest() throws Exception {
        RequestVoteRequest expected = new RequestVoteRequest(1, 2, 3, 4);
        RequestVoteRequest actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

    @Test
    void testSerializeRequestVoteResponse() throws Exception {
        RequestVoteResponse expected = new RequestVoteResponse(1, true);
        RequestVoteResponse actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

    @Test
    void testSerializeStateMachineRequest() throws Exception {
        StateMachineRequest expected = new StateMachineRequest("some-command");
        StateMachineRequest actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

    @Test
    void testSerializeStateMachineResponse() throws Exception {
        StateMachineResponse expected = new StateMachineResponse(true);
        StateMachineResponse actual = serializeDeserialize(expected);
        assertSerialized(expected, actual);
    }

}
