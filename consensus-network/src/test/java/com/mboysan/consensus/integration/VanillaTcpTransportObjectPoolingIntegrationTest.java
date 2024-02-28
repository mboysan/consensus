package com.mboysan.consensus.integration;


import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.MultiThreadExecutor;
import com.mboysan.consensus.util.TestUtils;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class VanillaTcpTransportObjectPoolingIntegrationTest extends VanillaTcpTransportTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaTcpTransportObjectPoolingIntegrationTest.class);

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @Test
    void testCreateMultipleTcpClients() {
        Properties server0Props = serverProperties(0);
        server0Props.put(TcpTransportConfig.Param.CLIENT_POOL_SIZE, "-1");
        server0Props.put(TcpTransportConfig.Param.MESSAGE_CALLBACK_TIMEOUT_MS, "-1");
        Properties server1Props = serverProperties(1);
        server1Props.put(TcpTransportConfig.Param.CLIENT_POOL_SIZE, "-1");
        server1Props.put(TcpTransportConfig.Param.MESSAGE_CALLBACK_TIMEOUT_MS, "-1");

        final VanillaTcpServerTransport sendingServer = createServerTransport(server0Props);
        final VanillaTcpServerTransport receivingServer = createServerTransport(server1Props);

        try {
            sendingServer.start();
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
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
            fail(e);
        } finally {
            teardownServers(sendingServer, receivingServer);
        }
        
    }

}
