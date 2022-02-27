package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.NettyTransportConfig;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KVStoreClientCLI {
    public static final Map<Integer, KVStoreClient> CLIENT_REFERENCES = new ConcurrentHashMap<>();
    public static volatile boolean testingInProgress = false;

    public static void main(String[] args) throws IOException {
        Properties mainProps = new Properties();

        for (String arg : args) {
            String[] kv = arg.split("=");
            mainProps.put(kv[0], kv[1]);
        }
        int clientId = resolveClientId(mainProps);

        NettyTransportConfig clientTransportConfig = Configuration.newInstance(NettyTransportConfig.class, mainProps);
        Transport clientTransport = new NettyClientTransport(clientTransportConfig);
        KVStoreClient client = new KVStoreClient(clientTransport);
        CLIENT_REFERENCES.put(clientId, client);

        Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));

        client.start();

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
            return new Random().nextInt();
        }
        return Integer.parseInt(clientId);
    }
}
