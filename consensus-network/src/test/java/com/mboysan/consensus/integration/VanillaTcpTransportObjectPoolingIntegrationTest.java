package com.mboysan.consensus.integration;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;

class VanillaTcpTransportObjectPoolingIntegrationTest extends VanillaTcpTransportTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpTransportObjectPoolingIntegrationTest.class);

    @Test
    void testCreateMultipleTcpClients() throws IOException, ExecutionException, InterruptedException {
        Properties server0Props = serverProperties(0);
        server0Props.put("transport.tcp.clientPoolSize", "-1");
        server0Props.put("transport.message.callbackTimeoutMs", "-1");
        Properties server1Props = serverProperties(1);
        server1Props.put("transport.tcp.clientPoolSize", "-1");
        server1Props.put("transport.message.callbackTimeoutMs", "-1");

        VanillaTcpServerTransport sendingServer = createServerTransport(server0Props);
        sendingServer.start();
        VanillaTcpServerTransport receivingServer = createServerTransport(server1Props);
        receivingServer.start();

        final int expectedTcpClientCount = 5;

        CyclicBarrier barrier = new CyclicBarrier(expectedTcpClientCount);
        AtomicInteger actualMsgCount = new AtomicInteger(0);
        UnaryOperator<Message> messageProcessor = (req) -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                LOGGER.error(e.getMessage(), e);
            }
            actualMsgCount.incrementAndGet();
            TestMessage request= (TestMessage) req;
            return new TestMessage(request.getPayload()).responseTo(req);
        };

        receivingServer.registerMessageProcessor(messageProcessor);

        try (MultiThreadExecutor executor = new MultiThreadExecutor(expectedTcpClientCount)) {
            for (int payloadId = 0; payloadId < expectedTcpClientCount; payloadId++) {
                TestMessage request = testMessage(payloadId, 0, 1);
                        executor.execute(() -> {
                            TestMessage response = (TestMessage) sendingServer.sendRecv(request);
                            assertResponse(request, response);
                        });
            }
        }

        assertEquals(expectedTcpClientCount, actualMsgCount.get());
    }

}
