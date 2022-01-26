package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoRPCProtocol;
import com.mboysan.consensus.NettyClientTransport;
import com.mboysan.consensus.NettyServerTransport;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.util.NettyUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NettyTransportIntegrationTest {

    private static final String HOST_NAME = "localhost";

    private static final int NUM_SERVERS = 3;
    private static final int NUM_CLIENTS = 3;
    private static final List<Destination> DESTINATIONS = new ArrayList<>();
    static {
        addDestination(0, NettyUtil.findFreePort());
        addDestination(1, NettyUtil.findFreePort());
        addDestination(2, NettyUtil.findFreePort());
    }
    private NettyServerTransport[] serverTransports;
    private NettyClientTransport[] clientTransports;

    @BeforeEach
    void setUp() throws IOException {
        setupServers();
        setupClients();
    }

    private void setupServers() throws IOException {
        serverTransports = new NettyServerTransport[NUM_SERVERS];
        for (int i = 0; i < serverTransports.length; i++) {
            int port = DESTINATIONS.get(i).getPort();
            NettyServerTransport serverTransport = createServerTransport(port);
            serverTransport.registerMessageProcessor(new EchoRPCProtocol());
            serverTransports[i] = serverTransport;
            serverTransport.start();
        }
    }

    private void setupClients() {
        clientTransports = new NettyClientTransport[NUM_CLIENTS];
        for (int i = 0; i < clientTransports.length; i++) {
            NettyClientTransport clientTransport = createClientTransport();
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
        for (NettyServerTransport serverTransport : serverTransports) {
            serverTransport.shutdown();
            assertTrue(serverTransport.verifyShutdown());
        }
    }

    private void teardownClients() {
        for (NettyClientTransport clientTransport : clientTransports) {
            clientTransport.shutdown();
            assertTrue(clientTransport.verifyShutdown());
        }
    }

    @Test
    void testSomeUnhappyPaths() {
        // --- server transport unhappy paths
        NettyServerTransport server = serverTransports[0];
        assertFalse(server.verifyShutdown());   // server should be running

        assertDoesNotThrow(server::start); // 2nd start does nothing (increase code cov)

        TestMessage testMsg1 = new TestMessage("");
        assertThrows(UnsupportedOperationException.class, () -> server.sendRecvAsync(testMsg1));

        server.shutdown();
        assertDoesNotThrow(server::shutdown); // 2nd shutdown does nothing (increase code cov)
        assertTrue(server.verifyShutdown());   // server should be shut down

        // --- client transport unhappy paths
        NettyClientTransport client = createClientTransport();
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
    void testMultiThreadedServerToServerComm() throws ExecutionException, InterruptedException {
        testMultithreadedComm(serverTransports);
    }

    @Test
    void testMultiThreadedClientToServerComm() throws ExecutionException, InterruptedException {
        testMultithreadedComm(clientTransports);
    }

    void testMultithreadedComm(Transport[] transports) throws ExecutionException, InterruptedException {
        MultiThreadExecutor executor = new MultiThreadExecutor();
        int msgCountPerPair = 10;
        int expectedMsgCount = msgCountPerPair * transports.length * transports.length;
        AtomicInteger actualMsgCount = new AtomicInteger(0);
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
        executor.endExecution();
        assertEquals(expectedMsgCount, actualMsgCount.get());
    }

    @Test
    void testIOErrorOnReceiverShutdown() throws IOException {
        TestMessage request = testMessage(0, 0, 1);

        serverTransports[1].shutdown();
        assertThrows(IOException.class, () -> serverTransports[0].sendRecv(request));

        serverTransports[1].start();
        TestMessage response = (TestMessage) serverTransports[0].sendRecv(request);
        assertResponse(request, response);
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

    NettyServerTransport createServerTransport(int port) {
        Properties properties = new Properties();
        properties.put("transport.netty.port", port + "");
        properties.put("transport.netty.destinations", NettyUtil.convertDestinationsListToProps(DESTINATIONS));
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyServerTransport(config);
    }

    NettyClientTransport createClientTransport() {
        Properties properties = new Properties();
        properties.put("transport.netty.destinations", NettyUtil.convertDestinationsListToProps(DESTINATIONS));
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyClientTransport(config);
    }

    private static void addDestination(int nodeId, int port) {
        DESTINATIONS.add(new Destination(nodeId, HOST_NAME, port));
    }
}
