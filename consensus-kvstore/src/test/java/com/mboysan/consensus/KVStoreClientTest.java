package com.mboysan.consensus;

import com.mboysan.consensus.message.KVIterateKeysResponse;
import com.mboysan.consensus.util.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KVStoreClientTest {

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @Test
    void testValidateResponseFail() throws Exception {
        KVIterateKeysResponse response = new KVIterateKeysResponse(false, new IllegalArgumentException(), null);

        Transport transport = mock(Transport.class);
        when(transport.getDestinationNodeIds()).thenReturn(Set.of(1));
        when(transport.sendRecv(any())).thenReturn(response);

        KVStoreClient client = new KVStoreClient(transport);
        assertThrows(KVOperationException.class, client::iterateKeys);
    }
}
