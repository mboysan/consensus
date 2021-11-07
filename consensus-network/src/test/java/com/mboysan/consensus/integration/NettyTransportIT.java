package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoRPCProtocol;
import com.mboysan.consensus.NettyClientTransport;
import com.mboysan.consensus.NettyServerTransport;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyTransportIT {

    private static final int NUM_SERVERS = 3;
    private static final String DESTINATIONS = "0=localhost:8080, 1=localhost:8081, 2=localhost:8082";
    private NettyServerTransport[] serverTransports;

    @BeforeEach
    void setUp() throws IOException {
        serverTransports = new NettyServerTransport[NUM_SERVERS];
        for (int i = 0; i < serverTransports.length; i++) {
            NettyServerTransport serverTransport = createServerTransport(8080 + i);
            serverTransport.addNode(i, new EchoRPCProtocol());
            serverTransports[i] = serverTransport;
            serverTransport.start();
        }
    }

    @AfterEach
    void tearDown() {
        for (NettyServerTransport serverTransport : serverTransports) {
            serverTransport.shutdown();
            assertTrue(serverTransport.verifyShutdown());
        }
    }

    @Test
    void testMultiThreadedServerToServerComm() throws ExecutionException, InterruptedException {
        MultiThreadExecutor executor = new MultiThreadExecutor();
        int msgCountPerPair = 10;
        int expectedMsgCount = msgCountPerPair * serverTransports.length * serverTransports.length;
        AtomicInteger actualMsgCount = new AtomicInteger(0);
        for (int senderId = 0; senderId < serverTransports.length; senderId++) {
            Transport sender = serverTransports[senderId];
            for (int receiverId = 0; receiverId < serverTransports.length; receiverId++) {
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
    void testClientToServerComm() throws IOException {
        NettyClientTransport client = createClientTransport();
        client.start();
        try {
            int msgCountPerPair = 10;
            for (int receiverId = 0; receiverId < serverTransports.length; receiverId++) {
                for (int payloadId = 0; payloadId < msgCountPerPair; payloadId++) {
                    TestMessage request = testMessage(payloadId, -1, receiverId);
                    TestMessage response = (TestMessage) client.sendRecv(request);
                    assertResponse(request, response);
                }
            }
        } finally {
            client.shutdown();
            assertTrue(client.verifyShutdown());
        }
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
        properties.put("transport.netty.destinations", DESTINATIONS);
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyServerTransport(config);
    }

    NettyClientTransport createClientTransport() {
        Properties properties = new Properties();
        properties.put("transport.netty.destinations", DESTINATIONS);
        // create new config per transport
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, properties);
        return new NettyClientTransport(config);
    }
}
