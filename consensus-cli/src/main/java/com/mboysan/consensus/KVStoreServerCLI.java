package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class KVStoreServerCLI {

    public static final Map<Integer, AbstractKVStore<?>> STORE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties mainProps = new Properties();

        for (String arg : args) {
            String[] kv = arg.split("=");
            mainProps.put(kv[0], kv[1]);
        }

        Transport nodeServingTransport = createNodeServingTransport(args, mainProps);
        Transport clientServingTransport = createClientServingTransport(args, mainProps);

        AbstractKVStore<?> kvStore;

        Configuration conf = Configuration.newInstance(Configuration.class, mainProps);
        switch (conf.nodeConsensusProtocol()) {
            case "raft" -> {
                RaftConfig raftConfig = Configuration.newInstance(RaftConfig.class, mainProps);
                RaftNode raftNode = new RaftNode(raftConfig, nodeServingTransport);
                kvStore = new RaftKVStore(raftNode, clientServingTransport);
            }
            case "bizur" -> {
                BizurConfig bizurConfig = Configuration.newInstance(BizurConfig.class, mainProps);
                BizurNode bizurNode = new BizurNode(bizurConfig, nodeServingTransport);
                kvStore = new BizurKVStore(bizurNode, clientServingTransport);
            }
            default -> throw new IllegalStateException("Unexpected value: " + conf.nodeConsensusProtocol());
        }
        STORE_REFERENCES.put(kvStore.getNode().getNodeId(), kvStore);

        Runtime.getRuntime().addShutdownHook(new Thread(kvStore::shutdown));

        kvStore.start().get();
    }

    private static Transport createNodeServingTransport(String[] args, Properties mainProps) {
        return createServerTransport(args, 0, mainProps);
    }

    private static Transport createClientServingTransport(String[] args, Properties mainProps) {
        return createServerTransport(args, 1, mainProps);
    }

    private static Transport createServerTransport(String[] args, int portIndex, Properties mainProps) {
        Properties transportProperties = new Properties();
        transportProperties.putAll(mainProps);
        for (String arg : args) {
            if (arg.contains("ports")) {
                arg = arg.substring(arg.indexOf("=") + 1);
                String[] ports = arg.split(",");
                transportProperties.put("transport.tcp.server.port", ports[portIndex] + "");
            }
        }
        TcpTransportConfig serverTransportConfig = Configuration.newInstance(TcpTransportConfig.class, transportProperties);
        return new VanillaTcpServerTransport(serverTransportConfig);
    }
}
