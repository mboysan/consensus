package com.mboysan.consensus.network;

import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class FailureDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);
    private boolean isRunning = false;
    private final VanillaTcpClientTransport clientTransport;
    private final Map<Integer, FailedServer> failedServerMap = new HashMap<>();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private final int markServerAsFailedCount;

    FailureDetector(
            VanillaTcpClientTransport clientTransport, TcpTransportConfig config, int associatedServerId) {
        this.clientTransport = clientTransport;
        this.markServerAsFailedCount = config.markServerAsFailedCount();

        final String target = associatedServerId > -1 ? "server-" + associatedServerId : "client";
        if (markServerAsFailedCount <= 0) {
            LOGGER.info("failure detection on {} disabled.", target);
            return;
        }
        LOGGER.info("failure detection on {} enabled with config={}.", target, config);
        this.isRunning = true;

        scheduledExecutor.scheduleAtFixedRate(this::checkFailedServers,
                0, config.pingInterval(), TimeUnit.MILLISECONDS);
    }

    private synchronized void checkFailedServers() {
        Iterator<Map.Entry<Integer, FailedServer>> iterator = failedServerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, FailedServer> entry = iterator.next();
            FailedServer failedServer = entry.getValue();
            if (failedServer.failCount >= markServerAsFailedCount) {
                // temporarily mark server as stable to be able to send a ping request.
                failedServer.markedStableTemporarily = true;
                try {
                    CustomRequest ping = new CustomRequest("ping").setReceiverId(failedServer.serverId);
                    clientTransport.sendRecv(ping);
                    // we received some response
                    iterator.remove();
                    LOGGER.info("server-{} marked stable again.", failedServer.serverId);
                } catch (IOException ignore) {
                } finally {
                    failedServer.markedStableTemporarily = false;
                }
            }
        }
    }

    synchronized void markFailed(int serverId) {
        if (!isRunning) {
            return;
        }
        FailedServer failedServer = failedServerMap.computeIfAbsent(serverId, FailedServer::new);
        failedServer.failCount++;
        if (failedServer.failCount == 1) {
            LOGGER.warn("server-{} marked as unstable, messages sent to this server will automatically " +
                    "fail until stability is ensured.", serverId);
        } else {
            LOGGER.debug("Server comm failed {}", failedServer);
        }
    }

    synchronized void validateStability(int serverId) throws IOException {
        if (!isRunning) {
            return;
        }
        FailedServer failedServer = failedServerMap.get(serverId);
        if (failedServer == null) {
            return;
        }
        if (failedServer.markedStableTemporarily) {
            return;
        }
        if (failedServer.failCount >= markServerAsFailedCount) {
            throw new IOException("server-" + serverId + " is unstable");
        }
    }

    synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        ShutdownUtil.shutdown(LOGGER, scheduledExecutor);
    }

    private static final class FailedServer {
        private final int serverId;
        private int failCount = 0;

        private boolean markedStableTemporarily = false;

        private FailedServer(int serverId) {
            this.serverId = serverId;
        }

        @Override
        public String toString() {
            return "FailedServer{" +
                    "serverId=" + serverId +
                    ", failCount=" + failCount +
                    '}';
        }
    }

}
