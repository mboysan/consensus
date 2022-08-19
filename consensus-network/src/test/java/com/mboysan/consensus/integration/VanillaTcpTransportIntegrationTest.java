package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoProtocolImpl;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VanillaTcpTransportIntegrationTest extends VanillaTcpTransportTestBase {

    private VanillaTcpServerTransport[] serverTransports;
    private VanillaTcpClientTransport[] clientTransports;

    @BeforeEach
    void setUp() {
        this.serverTransports = setupServers();
        this.clientTransports = setupClients();
    }

    @AfterEach
    void tearDown() {
        teardownServers(serverTransports);
        teardownClients(clientTransports);
    }

    @Test
    void testSomeUnhappyPaths() {
        // --- server transport unhappy paths
        VanillaTcpServerTransport server = serverTransports[0];
        assertFalse(server.verifyShutdown());   // server should be running

        assertDoesNotThrow(server::start); // 2nd start does nothing (increase code cov)

        TestMessage testMsg1 = new TestMessage("");
        assertThrows(UnsupportedOperationException.class, () -> server.sendRecvAsync(testMsg1));

        server.shutdown();
        assertDoesNotThrow(server::shutdown); // 2nd shutdown does nothing (increase code cov)
        assertTrue(server.verifyShutdown());   // server should be shut down

        // --- client transport unhappy paths
        VanillaTcpClientTransport client = createClientTransport();
        assertTrue(client.verifyShutdown());    // client is not started yet.

        TestMessage testMsg2 = new TestMessage("");
        assertThrows(IllegalStateException.class, () -> client.sendRecv(testMsg2));

        client.start();
        assertDoesNotThrow(client::start); // 2nd start does nothing (increase code cov)
        assertFalse(client.verifyShutdown());   // client should be running

        assertFalse(client.isShared());

        assertThrows(UnsupportedOperationException.class, () -> client.registerMessageProcessor(null));

        Message message = mock(Message.class);
        when(message.getId()).thenReturn(null); // msg id must be present
        assertThrows(IllegalArgumentException.class, () -> client.sendRecv(message));

        client.shutdown();
        assertDoesNotThrow(client::shutdown); // 2nd shutdown does nothing (increase code cov)
        assertTrue(client.verifyShutdown());   // server should be shut down
    }

    @Test
    void testClientToServerSimple() throws IOException {
        Transport sender = clientTransports[0];
        TestMessage request = testMessage(0, 0, 0);
        TestMessage response = (TestMessage) sender.sendRecv(request);
        assertResponse(request, response);
    }

    @Test
    void testMultiThreadClientToServerSimple() throws Exception {
        Transport sender = clientTransports[0];
        try (MultiThreadExecutor executor = new MultiThreadExecutor()) {
            for (int i = 0; i < 100; i++) {
                int finalI = i;
                executor.execute(() -> {
                    TestMessage request = testMessage(finalI, 0, 0);
                    TestMessage response = (TestMessage) sender.sendRecv(request);
                    assertResponse(request, response);
                });
            }
        }
    }

    @Test
    void testMultiThreadedServerToServerComm() throws ExecutionException, InterruptedException {
        testMultithreadedComm(serverTransports);
    }

    @Test
    void testMultiThreadedClientToServerComm() throws ExecutionException, InterruptedException {
        testMultithreadedComm(clientTransports);
    }

    void testMultithreadedComm(Transport[] transports) throws ExecutionException, InterruptedException {
        int msgCountPerPair = 10;
        int expectedMsgCount = msgCountPerPair * transports.length * transports.length;
        AtomicInteger actualMsgCount = new AtomicInteger(0);
        try (MultiThreadExecutor executor = new MultiThreadExecutor()) {
            for (int senderId = 0; senderId < transports.length; senderId++) {
                Transport sender = transports[senderId];
                for (int receiverId = 0; receiverId < transports.length; receiverId++) {
                    for (int payloadId = 0; payloadId < msgCountPerPair; payloadId++) {
                        TestMessage request = testMessage(payloadId, senderId, receiverId);
                        executor.execute(() -> {
                            TestMessage response = (TestMessage) sender.sendRecv(request);
                            assertResponse(request, response);
                            actualMsgCount.incrementAndGet();
                        });
                    }
                }
            }
        }
        assertEquals(expectedMsgCount, actualMsgCount.get());
    }

    @Test
    void testIOErrorOnReceiverShutdown() {
        TestMessage request = testMessage(0, 0, 1);

        serverTransports[1].shutdown();
        awaiting(() -> assertThrows(IOException.class, () -> serverTransports[0].sendRecv(request)));

        serverTransports[1].start();
        awaiting(() -> {
            TestMessage response = (TestMessage) serverTransports[0].sendRecv(request);
            assertResponse(request, response);
        });
    }

    @Test
    void testSocketTimeout() {
        Transport sender = clientTransports[0];
        // signal the server to wait before responding, this will end up socket to timeout before receiving a response.
        TestMessage request = new TestMessage(EchoProtocolImpl.PAYLOAD_WAIT)
                .setSenderId(0)
                .setReceiverId(1);
        assertThrows(IOException.class, () -> sender.sendRecv(request));
    }

    private TestMessage testMessage(int payloadId, int senderId, int receiverId) {
        String payload = "some-payload-" + payloadId;
        return new TestMessage(payload).setSenderId(senderId).setReceiverId(receiverId);
    }

    private void assertResponse(TestMessage request, TestMessage response) {
        assertEquals(request.getPayload(), response.getPayload());
        assertEquals(request.getId(), response.getId());
        assertEquals(request.getSenderId(), response.getReceiverId());
        assertEquals(request.getReceiverId(), response.getSenderId());
    }
}
