package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.netty.NettyClientTransport;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
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

        TcpTransportConfig clientTransportConfig = Configuration.newInstance(TcpTransportConfig.class, mainProps);
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
            return new SecureRandom().nextInt();
        }
        return Integer.parseInt(clientId);
    }
}
