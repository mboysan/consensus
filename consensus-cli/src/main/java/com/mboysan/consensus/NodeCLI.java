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

public class NodeCLI {

    public static final Map<Integer, AbstractNode<?>> NODE_REFERENCES = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties mainProps = new Properties();

        for (String arg : args) {
            String[] kv = arg.split("=");
            mainProps.put(kv[0], kv[1]);
        }

        TcpTransportConfig serverTransportConfig = Configuration.newInstance(TcpTransportConfig.class, mainProps);
        Transport nodeServingTransport = new VanillaTcpServerTransport(serverTransportConfig);

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

        node.start().get();
    }
}
