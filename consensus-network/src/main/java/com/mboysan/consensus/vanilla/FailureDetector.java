package com.mboysan.consensus.vanilla;

import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.util.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
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
                failedServer.markedStableTemporarily = true;   // temporarily mark server as stable
                CustomRequest ping = new CustomRequest("ping").setReceiverId(failedServer.serverId);
                try {
                    clientTransport.sendRecv(ping);
                    // we received some response
                    iterator.remove();
                } catch (IOException e) {
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
        LOGGER.debug("Server marked as failed {}", failedServer);
    }

    synchronized void validateWorking(int serverId) throws IOException {
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
            LOGGER.debug("Server is unstable {}", failedServer);
            throw new IOException("server-" + serverId + " is unstable");
        }
    }

    synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        shutdown(scheduledExecutor::shutdown);
        shutdown(() -> scheduledExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS));
    }

    private static void shutdown(ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        }
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
