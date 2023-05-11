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

public class KVStoreServerCLI {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreServerCLI.class);

    private static final Map<Integer, AbstractKVStore<?>> STORE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try {
            main0(args);
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static void main0(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties nodeSectionProperties = CliArgsHelper.getNodeSectionProperties(args);
        TcpTransportConfig nodeServingTransportConfig
                = CoreConfig.newInstance(TcpTransportConfig.class, nodeSectionProperties);
        Transport nodeServingTransport = new VanillaTcpServerTransport(nodeServingTransportConfig);

        Properties storeSectionProperties = CliArgsHelper.getStoreSectionProperties(args);
        TcpTransportConfig clientServingTransportConfig
                = CoreConfig.newInstance(TcpTransportConfig.class, storeSectionProperties);
        Transport clientServingTransport = new VanillaTcpServerTransport(clientServingTransportConfig);

        AbstractKVStore<?> kvStore;

        NodeConfig conf = CoreConfig.newInstance(NodeConfig.class, nodeSectionProperties);
        switch (conf.nodeConsensusProtocol()) {
            case "raft" -> {
                RaftConfig raftConfig = CoreConfig.newInstance(RaftConfig.class, nodeSectionProperties);
                RaftNode raftNode = new RaftNode(raftConfig, nodeServingTransport);
                kvStore = new RaftKVStore(raftNode, clientServingTransport);
            }
            case "bizur" -> {
                BizurConfig bizurConfig = CoreConfig.newInstance(BizurConfig.class, nodeSectionProperties);
                BizurNode bizurNode = new BizurNode(bizurConfig, nodeServingTransport);
                kvStore = new BizurKVStore(bizurNode, clientServingTransport);
            }
            case "simulate" -> {
                SimConfig simConfig = CoreConfig.newInstance(SimConfig.class, nodeSectionProperties);
                SimNode simNode = new SimNode(simConfig, nodeServingTransport);
                kvStore = new SimKVStore(simNode, clientServingTransport);
            }
            default -> throw new IllegalStateException("Unexpected value: " + conf.nodeConsensusProtocol());
        }
        STORE_REFERENCES.put(kvStore.getNode().getNodeId(), kvStore);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(kvStore));

        startMetricsCollector(CliArgsHelper.getProperties(args), kvStore);

        kvStore.start().get();
        LOGGER.info("store started");
    }

    private static Thread createShutdownHookThread(AbstractKVStore<?> kvStore) {
        return new Thread(() -> {
            try {
                kvStore.shutdown();
            } finally {
                BackgroundServiceRegistry.getInstance().shutdownAll();
            }
        });
    }

    private static void startMetricsCollector(Properties properties, AbstractKVStore<?> kvStore) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        MetricsCollectorService metricsCollectorService = MetricsCollectorService.initAndStart(config);
        metricsCollectorService.scheduleCustomReporter(() -> kvStore.dumpStoreMetricsAsync());
    }

    public static AbstractKVStore<?> getStore(int nodeId) {
        return STORE_REFERENCES.get(nodeId);
    }

    public static Collection<AbstractKVStore<?>> getStores() {
        return STORE_REFERENCES.values();
    }
}
