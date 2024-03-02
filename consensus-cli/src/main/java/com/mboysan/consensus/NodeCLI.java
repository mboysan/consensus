package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.util.CliArgsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class NodeCLI {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCLI.class);
    private static final Map<Integer, AbstractNode<?>> NODE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try {
            main0(args);
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static void main0(String[] args) throws IOException, ExecutionException, InterruptedException {
        CliArgsHelper.logArgs(args);

        Properties properties = CliArgsHelper.getProperties(args);

        AbstractNode<?> node = CLIFactory.createNode(properties);
        NODE_REFERENCES.put(node.getNodeId(), node);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(node));

        startMetricsCollector(properties);

        node.start().get();
        LOGGER.info("node started");
    }

    private static Thread createShutdownHookThread(AbstractNode<?> node) {
        return new Thread(() -> {
            try {
                node.shutdown();
            } finally {
                BackgroundServiceRegistry.getInstance().shutdownAll();
                LOGGER.info("Completed shutdown hook on node-{}", node.getNodeId());
            }
        });
    }

    private static void startMetricsCollector(Properties properties) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        MetricsCollectorService.initAndStart(config);
    }

    static AbstractNode<?> getNode(int nodeId) {
        return NODE_REFERENCES.get(nodeId);
    }

    static Collection<AbstractNode<?>> getNodes() {
        return NODE_REFERENCES.values();
    }
}
