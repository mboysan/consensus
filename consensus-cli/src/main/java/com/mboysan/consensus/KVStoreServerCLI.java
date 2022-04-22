package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.RaftConfig;
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

public class KVStoreServerCLI {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreServerCLI.class);

    private static final AtomicReference<MetricsCollector> METRICS_COLLECTOR_REF = new AtomicReference<>();

    private static final Map<Integer, AbstractKVStore<?>> STORE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        startMetricsCollector(CliArgsHelper.getProperties(args));

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
            default -> throw new IllegalStateException("Unexpected value: " + conf.nodeConsensusProtocol());
        }
        STORE_REFERENCES.put(kvStore.getNode().getNodeId(), kvStore);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(kvStore));

        kvStore.start().get();
        LOGGER.info("store started");
    }

    private static Thread createShutdownHookThread(AbstractKVStore<?> kvStore) {
        return new Thread(() -> {
            try {
                kvStore.shutdown();
            } finally {
                LOGGER.info("store stopped");
                closeMetricsCollector();
            }
        });
    }

    private static void startMetricsCollector(Properties properties) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        METRICS_COLLECTOR_REF.compareAndSet(null, MetricsCollector.initAndStart(config));
    }

    private static void closeMetricsCollector() {
        METRICS_COLLECTOR_REF.get().close();
    }

    public static AbstractKVStore<?> getStore(int nodeId) {
        return STORE_REFERENCES.get(nodeId);
    }

    public static Collection<AbstractKVStore<?>> getStores() {
        return STORE_REFERENCES.values();
    }
}
