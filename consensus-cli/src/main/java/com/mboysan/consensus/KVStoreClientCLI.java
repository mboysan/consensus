package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.util.CliArgsHelper;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class KVStoreClientCLI {

    public static volatile boolean testingInProgress = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClientCLI.class);

    private static final AtomicReference<MetricsCollector> METRICS_COLLECTOR_REF = new AtomicReference<>();

    private static final Map<Integer, KVStoreClient> CLIENT_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        try {
            main0(args);
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    private static void main0(String[] args) throws IOException {
        Properties properties = CliArgsHelper.getProperties(args);

        startMetricsCollector(properties);

        int clientId = resolveClientId(properties);

        TcpTransportConfig clientTransportConfig = CoreConfig.newInstance(TcpTransportConfig.class, properties);
        Transport clientTransport = new VanillaTcpClientTransport(clientTransportConfig);
        KVStoreClient client = new KVStoreClient(clientTransport);
        CLIENT_REFERENCES.put(clientId, client);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(client));

        client.start();
        LOGGER.info("client started");

        if (!testingInProgress) {
            System.out.println("client ready to receive commands:");

            Scanner scanner = new Scanner(System.in);
            boolean exited = false;
            while (!exited) {
                try {
                    String input = scanner.nextLine();
                    String[] cmd = input.split(" ");
                    switch (cmd[0]) {
                        case "set" -> client.set(cmd[1], cmd[2]);
                        case "get" -> System.out.println("result -> " + client.get(cmd[1]));
                        case "delete" -> client.delete(cmd[1]);
                        case "iterateKeys" -> System.out.println("result -> " + client.iterateKeys());
                        case "exit" -> exited = true;
                        default -> throw new IllegalArgumentException("command invalid");
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
            client.shutdown();
        }
    }

    private static int resolveClientId(Properties mainProps) {
        String clientId = mainProps.getProperty("client.id");
        if (clientId == null) {
            return new SecureRandom().nextInt();
        }
        return Integer.parseInt(clientId);
    }

    private static Thread createShutdownHookThread(KVStoreClient client) {
        return new Thread(() -> {
            try {
                client.shutdown();
            } finally {
                LOGGER.info("client stopped");
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

    public static KVStoreClient getClient(int id) {
        return CLIENT_REFERENCES.get(id);
    }

    public static Collection<KVStoreClient> getClients() {
        return CLIENT_REFERENCES.values();
    }
}
