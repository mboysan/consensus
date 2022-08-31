package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.util.CliArgsHelper;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class NodeCLI {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCLI.class);

    private static final AtomicReference<MetricsCollector> METRICS_COLLECTOR_REF = new AtomicReference<>();

    private static final Map<Integer, AbstractNode<?>> NODE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try {
            main0(args);
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static void main0(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties properties = CliArgsHelper.getProperties(args);

        startMetricsCollector(properties);

        TcpTransportConfig serverTransportConfig = CoreConfig.newInstance(TcpTransportConfig.class, properties);
        Transport nodeServingTransport = new VanillaTcpServerTransport(serverTransportConfig);

        AbstractNode<?> node;

        NodeConfig conf = CoreConfig.newInstance(NodeConfig.class, properties);
        switch (conf.nodeConsensusProtocol()) {
            case "raft" -> {
                RaftConfig raftConfig = CoreConfig.newInstance(RaftConfig.class, properties);
                node = new RaftNode(raftConfig, nodeServingTransport);
            }
            case "bizur" -> {
                BizurConfig bizurConfig = CoreConfig.newInstance(BizurConfig.class, properties);
                node = new BizurNode(bizurConfig, nodeServingTransport);
            }
            case "simulate" -> {
                SimConfig simConfig = CoreConfig.newInstance(SimConfig.class, properties);
                node = new SimNode(simConfig, nodeServingTransport);
            }
            default -> throw new IllegalStateException("Unexpected value: " + conf.nodeConsensusProtocol());
        }
        NODE_REFERENCES.put(node.getNodeId(), node);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(node));

        node.start().get();
        LOGGER.info("node started");
    }

    private static Thread createShutdownHookThread(AbstractNode<?> node) {
        return new Thread(() -> {
            try {
                node.shutdown();
            } finally {
                LOGGER.info("node stopped");
                shutdownBackgroundServices();
            }
        });
    }

    private static void startMetricsCollector(Properties properties) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        METRICS_COLLECTOR_REF.compareAndSet(null, MetricsCollector.initAndStart(config));
    }

    private static void shutdownBackgroundServices() {
        METRICS_COLLECTOR_REF.get().close();
        EventManager.getInstance().shutdown();
    }

    public static AbstractNode<?> getNode(int nodeId) {
        return NODE_REFERENCES.get(nodeId);
    }

    public static Collection<AbstractNode<?>> getNodes() {
        return NODE_REFERENCES.values();
    }
}
