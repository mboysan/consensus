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
        CliArgsHelper.logArgs(args);

        Properties allProperties = CliArgsHelper.getProperties(args);
        Properties storeProperties = CliArgsHelper.getStoreSectionProperties(args);
        Properties nodeProperties = CliArgsHelper.getNodeSectionProperties(args);

        AbstractKVStore<?> kvStore = CLIFactory.createKVStore(storeProperties, nodeProperties);
        STORE_REFERENCES.put(kvStore.getNode().getNodeId(), kvStore);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(kvStore));

        startMetricsCollector(allProperties, kvStore);

        kvStore.start().get();
        LOGGER.info("store started");
    }

    private static Thread createShutdownHookThread(AbstractKVStore<?> kvStore) {
        return new Thread(() -> {
            try {
                kvStore.shutdown();
            } finally {
                BackgroundServiceRegistry.getInstance().shutdownAll();
                LOGGER.info("Completed shutdown hook on store-{}", kvStore.getNode().getNodeId());
            }
        });
    }

    private static void startMetricsCollector(Properties properties, AbstractKVStore<?> kvStore) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        MetricsCollectorService metricsCollectorService = MetricsCollectorService.initAndStart(config);
        metricsCollectorService.registerCustomReporter(kvStore::dumpStoreMetricsAsync);
    }

    static AbstractKVStore<?> getStore(int nodeId) {
        return STORE_REFERENCES.get(nodeId);
    }

    static Collection<AbstractKVStore<?>> getStores() {
        return STORE_REFERENCES.values();
    }
}
