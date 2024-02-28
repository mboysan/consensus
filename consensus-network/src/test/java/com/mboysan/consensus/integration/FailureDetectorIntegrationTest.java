package com.mboysan.consensus.integration;

import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.TestMessage;
import com.mboysan.consensus.util.ShutdownUtil;
import com.mboysan.consensus.util.TestUtils;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.mboysan.consensus.util.AwaitUtil.doSleep;
import static org.junit.jupiter.api.Assertions.fail;

class FailureDetectorIntegrationTest extends VanillaTcpTransportTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorIntegrationTest.class);

    @BeforeEach
    void setUp(TestInfo testInfo) {
        TestUtils.logTestName(testInfo);
    }

    @Test
    void testFailureDetectionDisabled() throws IOException, InterruptedException {
        final long pingInterval = 100;

        Properties clientProperties = clientProperties();
        clientProperties.put(TcpTransportConfig.Param.MARK_SERVER_AS_FAILED_COUNT, String.valueOf(-1));
        clientProperties.put(TcpTransportConfig.Param.PING_INTERVAL, String.valueOf(pingInterval));
        final VanillaTcpClientTransport clientTransport = createClientTransport(clientProperties);

        final VanillaTcpServerTransport serverTransport = createServerTransport(0);
        try {
            clientTransport.start();

            CountDownLatch latch = new CountDownLatch(1);
            serverTransport.registerMessageProcessor(message -> {
                if (message instanceof TestMessage) {
                    doSleep(pingInterval * 2);
                    latch.countDown();
                }
                if (message instanceof CustomRequest request) {
                    fail("ping request must not have been received, request=" + request);
                }
                return new CustomResponse(true, null, null).responseTo(message);
            });
            serverTransport.start();

            clientTransport.sendRecv(new TestMessage("").setReceiverId(0));
            latch.await();
        } finally {
            ShutdownUtil.shutdown(LOGGER, clientTransport::shutdown);
            ShutdownUtil.shutdown(LOGGER, serverTransport::shutdown);
        }
    }

    @Test
    void testFailureDetectionEnabled() throws InterruptedException {
        final long pingInterval = 100;

        Properties clientProperties = clientProperties();
        clientProperties.put(TcpTransportConfig.Param.MARK_SERVER_AS_FAILED_COUNT, String.valueOf(1));    // 1 failure is enough.
        clientProperties.put(TcpTransportConfig.Param.PING_INTERVAL, String.valueOf(pingInterval));
        final VanillaTcpClientTransport clientTransport = createClientTransport(clientProperties);

        final VanillaTcpServerTransport serverTransport = createServerTransport(0);
        try {
            clientTransport.start();

            CountDownLatch latch = new CountDownLatch(1);
            serverTransport.registerMessageProcessor(message -> {
                if (message instanceof CustomRequest) { // ping request
                    latch.countDown();
                }
                return new CustomResponse(true, null, null).responseTo(message);
            });
            // don't start the server yet and try to send a few messages to the server.
            // The failure detector will understand that the client couldn't communicate with the server
            // and start sending ping requests.
            for (int i = 0; i < 5; i++) {
                try {
                    clientTransport.sendRecv(new TestMessage(i + "").setReceiverId(0));
                    fail("the message must not have been sent.");
                } catch (IOException ignore) {}
            }

            doSleep(pingInterval * 10); // wait for the ping messages to be sent.

            serverTransport.start();
            latch.await();
        } finally {
            ShutdownUtil.shutdown(LOGGER, clientTransport::shutdown);
            ShutdownUtil.shutdown(LOGGER, serverTransport::shutdown);
        }
    }

}
