package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.Configuration;
import com.mboysan.consensus.configuration.NettyTransportConfig;
import com.mboysan.consensus.configuration.RaftConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KVStoreServerCLI extends CLIBase {

    private static final Map<Integer, Future<Void>> START_FUTURES = new ConcurrentHashMap<>();

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

        Future<Void> startFuture = kvStore.start();
        if (testingInProgress) {
            START_FUTURES.put(kvStore.getNode().getNodeId(), startFuture);
        } else {
            startFuture.get();
        }
    }

    static void sync() {
        START_FUTURES.forEach((i,f) -> exec(f::get));
    }

    private static Transport createNodeServingTransport(String[] args, Properties mainProps) {
        return createNodeServingTransport(args, "node.transport", mainProps);
    }

    private static Transport createClientServingTransport(String[] args, Properties mainProps) {
        return createNodeServingTransport(args, "store.transport", mainProps);
    }

    private static Transport createNodeServingTransport(String[] args, String startString, Properties mainProps) {
        Properties transportProperties = new Properties();
        transportProperties.putAll(mainProps);
        for (String arg : args) {
            if (arg.startsWith(startString)) {
                arg = arg.substring(arg.indexOf(".") + 1);
                String[] kv = arg.split("=");
                transportProperties.put(kv[0], kv[1]);
            }
        }
        NettyTransportConfig serverTransportConfig = Configuration.newInstance(NettyTransportConfig.class, transportProperties);
        return new NettyServerTransport(serverTransportConfig);
    }
}
