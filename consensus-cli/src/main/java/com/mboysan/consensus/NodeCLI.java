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

public class NodeCLI extends CLIBase {

    private static final Map<Integer, Future<Void>> START_FUTURES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties mainProps = new Properties();

        for (String arg : args) {
            String[] kv = arg.split("=");
            mainProps.put(kv[0], kv[1]);
        }

        NettyTransportConfig serverTransportConfig = Configuration.newInstance(NettyTransportConfig.class, mainProps);
        Transport nodeServingTransport = new NettyServerTransport(serverTransportConfig);

        AbstractNode<?> node;

        Configuration conf = Configuration.newInstance(Configuration.class, mainProps);
        switch (conf.nodeConsensusProtocol()) {
            case "raft" -> {
                RaftConfig raftConfig = Configuration.newInstance(RaftConfig.class, mainProps);
                node = new RaftNode(raftConfig, nodeServingTransport);
            }
            case "bizur" -> {
                BizurConfig bizurConfig = Configuration.newInstance(BizurConfig.class, mainProps);
                node = new BizurNode(bizurConfig, nodeServingTransport);
            }
            default -> throw new IllegalStateException("Unexpected value: " + conf.nodeConsensusProtocol());
        }
        NODE_REFERENCES.put(node.getNodeId(), node);

        Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));

        Future<Void> startFuture = node.start();
        if (testingInProgress) {
            START_FUTURES.put(node.getNodeId(), startFuture);
        } else {
            startFuture.get();
        }
    }

    static void sync() {
        START_FUTURES.forEach((i,f) -> exec(f::get));
    }
}
