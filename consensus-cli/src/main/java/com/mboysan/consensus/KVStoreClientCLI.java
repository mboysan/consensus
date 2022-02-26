package com.mboysan.consensus;

import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class KVStoreClientCLI extends CLIBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreClientCLI.class);

    public static void main(String[] args) throws IOException {
        Properties mainProps = new Properties();

        for (String arg : args) {
            String[] kv = arg.split("=");
            mainProps.put(kv[0], kv[1]);
        }

        NettyTransportConfig clientTransportConfig = Configuration.newInstance(NettyTransportConfig.class, mainProps);
        Transport clientTransport = new NettyClientTransport(clientTransportConfig);
        KVStoreClient client = new KVStoreClient(clientTransport);
        CLIENT_REFERENCES.put(clientTransportConfig.nodeId(), client);

        Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));

        client.start();

        if (!testingInProgress) {
            LOGGER.info("client ready to receive commands:");

            Scanner scanner = new Scanner(System.in);
            boolean exited = false;
            while (!exited) {
                try {
                    String input = scanner.nextLine();
                    String[] cmd = input.split(" ");
                    switch (cmd[0]) {
                        case "set" -> client.set(cmd[1], cmd[2]);
                        case "get" -> LOGGER.info("result -> {}", client.get(cmd[1]));
                        case "delete" -> client.delete(cmd[1]);
                        case "iterate" -> LOGGER.info("result -> {}", client.iterateKeys());
                        case "exit" -> exited = true;
                        default -> throw new IllegalArgumentException("command invalid");
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            client.shutdown();
        }
    }
}
