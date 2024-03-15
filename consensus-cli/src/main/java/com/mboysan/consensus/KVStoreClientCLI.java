package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CliClientConfig;
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
        CliClientConfig cliClientConfig = CoreConfig.newInstance(CliClientConfig.class, properties);
        LOGGER.info("CliClientConfig={}", cliClientConfig);

        int clientId = resolveClientId(properties);
        KVStoreClient client = CLIFactory.createKVStoreClient(properties);
        CLIENT_REFERENCES.put(clientId, client);

        Runtime.getRuntime().addShutdownHook(createShutdownHookThread(client));

        startMetricsCollector(properties);

        client.start();
        LOGGER.info("client started");

        boolean isOneOffCommand = cliClientConfig.command() != null;
        boolean isInteractiveSession = cliClientConfig.interactive() && !isOneOffCommand;

        if (isOneOffCommand) {
            LOGGER.info("Sending one-off command");
            handle(client, cliClientConfig.command(), cliClientConfig);
            client.shutdown();
        } else if (isInteractiveSession) {
            LOGGER.info("Interactive session started. Client ready to receive commands:");
            try (Scanner scanner = new Scanner(System.in)) {
                boolean exited = false;
                while (!exited) {
                    try {
                        String input = scanner.nextLine();
                        String[] cmd = input.split(" ");
                        CliClientConfig cliArgs = parseCliClientConfig(cmd);
                        String command = cliArgs.command() == null ? cmd[0] : cliArgs.command();

                        int returnCode = handle(client, command, cliArgs);

                        exited = returnCode == 1;
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            client.shutdown();
        }
        // otherwise, keep the client running for Integration Tests.
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

    private static int handle(KVStoreClient client, String command, CliClientConfig cliArgs) throws CommandException {
        int returnCode = 0;
        switch (command) {
            case "set" -> {
                client.set(cliArgs.key(), cliArgs.value());
                printResult("OK");
            }
            case "get" -> {
                String result = client.get(cliArgs.key());
                printResult(result);
            }
            case "delete" -> {
                client.delete(cliArgs.key());
                printResult("OK");
            }
            case "iterateKeys" -> {
                Set<String> result = client.iterateKeys();
                printResult(result);
            }
            case "checkIntegrity" -> {
                String result = client.checkIntegrity(cliArgs.level(), cliArgs.routeTo());
                printResult(result);
            }
            case "exit" -> returnCode = 1;
            default -> sendCustomCommand(client, command, cliArgs.arguments(), cliArgs.routeTo());
        }
        return returnCode;
    }

    private static void sendCustomCommand(KVStoreClient client, String command, String arguments, int routeTo) {
        try {
            String result = client.customRequest(command, arguments, routeTo);
            printResult(result);
        } catch (CommandException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static CliClientConfig parseCliClientConfig(String[] args) {
        Properties properties = CliArgsHelper.getProperties(args);
        CliClientConfig cliClientConfig = CoreConfig.newInstance(CliClientConfig.class, properties);
        LOGGER.debug("CliClientConfig={}", cliClientConfig);
        return cliClientConfig;
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
