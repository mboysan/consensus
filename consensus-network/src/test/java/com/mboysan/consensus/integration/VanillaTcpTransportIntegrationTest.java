package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoRPCProtocol;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.util.NetUtil;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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

class VanillaTcpTransportIntegrationTest {

    private static final String HOST_NAME = "localhost";

    private static final int NUM_SERVERS = 3;
    private static final int NUM_CLIENTS = 3;
    private static final List<Destination> DESTINATIONS = new ArrayList<>();
    static {
        addDestination(0, NetUtil.findFreePort());
        addDestination(1, NetUtil.findFreePort());
        addDestination(2, NetUtil.findFreePort());
    }
    private VanillaTcpServerTransport[] serverTransports;
    private VanillaTcpClientTransport[] clientTransports;

    @BeforeEach
    void setUp() throws IOException {
        setupServers();
        setupClients();
    }

    private void setupServers() throws IOException {
        serverTransports = new VanillaTcpServerTransport[NUM_SERVERS];
        for (int i = 0; i < serverTransports.length; i++) {
            int port = DESTINATIONS.get(i).port();
            VanillaTcpServerTransport serverTransport = createServerTransport(port);
            serverTransport.registerMessageProcessor(new EchoRPCProtocol());
            serverTransports[i] = serverTransport;
            serverTransport.start();
        }
    }

    private void setupClients() {
        clientTransports = new VanillaTcpClientTransport[NUM_CLIENTS];
        for (int i = 0; i < clientTransports.length; i++) {
            VanillaTcpClientTransport clientTransport = createClientTransport();
            clientTransports[i] = clientTransport;
            clientTransport.start();
        }
    }

    @AfterEach
    void tearDown() {
        teardownServers();
        teardownClients();
    }

    private void teardownServers() {
        for (VanillaTcpServerTransport serverTransport : serverTransports) {
            serverTransport.shutdown();
            assertTrue(serverTransport.verifyShutdown());
        }
    }

    private void teardownClients() {
        for (VanillaTcpClientTransport clientTransport : clientTransports) {
            clientTransport.shutdown();
            assertTrue(clientTransport.verifyShutdown());
        }
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
    void testIOErrorOnReceiverShutdown() throws IOException {
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
        TestMessage request = new TestMessage("wait")
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

    VanillaTcpServerTransport createServerTransport(int port) {
        Properties properties = new Properties();
        properties.put("transport.tcp.server.port", String.valueOf(port));
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        properties.put("transport.tcp.server.socket.so_timeout", String.valueOf(2500)); // 2.5 seconds timeout
        // create new config per transport
        TcpTransportConfig config = CoreConfig.newInstance(TcpTransportConfig.class, properties);
        return new VanillaTcpServerTransport(config);
    }

    VanillaTcpClientTransport createClientTransport() {
        Properties properties = new Properties();
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        // create new config per transport
        TcpTransportConfig config = CoreConfig.newInstance(TcpTransportConfig.class, properties);
        return new VanillaTcpClientTransport(config);
    }

    private static void addDestination(int nodeId, int port) {
        DESTINATIONS.add(new Destination(nodeId, HOST_NAME, port));
    }
}
