package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.message.CommandException;
import com.mboysan.consensus.util.CliArgsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KVStoreClientCLI {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClientCLI.class);

    private static final String ROUTE_TO_SEPARATOR = "#";

    private static final Map<Integer, KVStoreClient> CLIENT_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        try {
            main0(args);
        } catch (RuntimeException | CommandException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static void main0(String[] args) throws IOException, CommandException {
        CliArgsHelper.logArgs(args);

        Properties properties = CliArgsHelper.getProperties(args);

        int clientId = resolveClientId(properties);
        KVStoreClient client = CLIFactory.createKVStoreClient(properties);
        CLIENT_REFERENCES.put(clientId, client);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(client));

        startMetricsCollector(properties);

        client.start();
        LOGGER.info("client started");

        if (isInteractiveSession(properties)) {
            LOGGER.info("client ready to receive commands:");

            try (Scanner scanner = new Scanner(System.in)) {
                boolean exited = false;
                while (!exited) {
                    try {
                        String input = scanner.nextLine();
                        String[] cmd = input.split(" ");
                        switch (cmd[0]) {
                            case "set" -> {
                                client.set(cmd[1], cmd[2]);
                                printResult("OK");
                            }
                            case "get" -> {
                                String result = client.get(cmd[1]);
                                printResult(result);
                            }
                            case "delete" -> {
                                client.delete(cmd[1]);
                                printResult("OK");
                            }
                            case "iterateKeys" -> {
                                Set<String> result = client.iterateKeys();
                                printResult(result);
                            }
                            case "exit" -> exited = true;
                            default -> sendCustomCommand(client, input);
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            client.shutdown();
        } else {
            String commandFromCli = properties.getProperty("command");
            if (commandFromCli != null) {
                sendCustomCommand(client, commandFromCli);
                client.shutdown();
            }
            // otherwise, keep the client running for Integration Tests.
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
                BackgroundServiceRegistry.getInstance().shutdownAll();
            }
        });
    }

    private static void startMetricsCollector(Properties properties) {
        MetricsConfig config = CoreConfig.newInstance(MetricsConfig.class, properties);
        MetricsCollectorService.initAndStart(config);
    }

    private static boolean isInteractiveSession(Properties properties) {
        String interactiveFlag = properties.getProperty("interactive");
        if ("false".equals(interactiveFlag)) {
            return false;
        }
        String commandFromCli = properties.getProperty("command");
        return commandFromCli == null;
    }

    private static void sendCustomCommand(KVStoreClient client, String input) throws CommandException {
        String[] request = prepareRequest(input);
        int routeToId = Integer.parseInt(request[0]);
        String command = request[1];
        String result = client.customRequest(command, routeToId);
        printResult(result);
    }

    private static String[] prepareRequest(String command) {
        if (!command.contains(ROUTE_TO_SEPARATOR)) {
            //returns: -1#<command>
            command = "%d%s%s".formatted(-1, ROUTE_TO_SEPARATOR, command);
        }
        return command.split(ROUTE_TO_SEPARATOR);
    }

    private static void printResult(Object result) {
        LOGGER.info("result: {}", result);
    }

    static KVStoreClient getClient(int id) {
        return CLIENT_REFERENCES.get(id);
    }

    static Collection<KVStoreClient> getClients() {
        return CLIENT_REFERENCES.values();
    }
}
