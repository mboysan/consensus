package com.mboysan.consensus;

import com.mboysan.consensus.configuration.BizurConfig;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.RaftConfig;
import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;

import java.util.Properties;

import static com.mboysan.consensus.CliConstants.Protocol.BIZUR;
import static com.mboysan.consensus.CliConstants.Protocol.RAFT;
import static com.mboysan.consensus.CliConstants.Protocol.SIMULATE;

public final class CLIFactory {
    private CLIFactory() {
    }

    static KVStoreClient createKVStoreClient(Properties properties) {
        Transport clientTransport = createTcpClientTransport(properties);
        return new KVStoreClient(clientTransport);
    }

    @SuppressWarnings("unchecked")
    static <T extends AbstractKVStore<?>> T createKVStore(Properties storeProperties, Properties nodeProperties) {
        Transport clientServingTransport = createTcpServerTransport(storeProperties);
        NodeConfig nodeConfig = CoreConfig.newInstance(NodeConfig.class, nodeProperties);
        return switch (nodeConfig.nodeConsensusProtocol()) {
            case RAFT -> (T) new RaftKVStore(createNode(nodeProperties), clientServingTransport);
            case BIZUR -> (T) new BizurKVStore(createNode(nodeProperties), clientServingTransport);
            case SIMULATE -> (T) new SimKVStore(createNode(nodeProperties), clientServingTransport);
            default -> throw new IllegalArgumentException("Unknown protocol: " + nodeConfig.nodeConsensusProtocol());
        };
    }

    @SuppressWarnings("unchecked")
    static <T extends AbstractNode<?>> T createNode(Properties properties) {
        Transport nodeServingTransport = createTcpServerTransport(properties);
        NodeConfig nodeConfig = CoreConfig.newInstance(NodeConfig.class, properties);
        return switch (nodeConfig.nodeConsensusProtocol()) {
            case RAFT -> (T) new RaftNode(CoreConfig.newInstance(RaftConfig.class, properties), nodeServingTransport);
            case BIZUR -> (T) new BizurNode(CoreConfig.newInstance(BizurConfig.class, properties), nodeServingTransport);
            case SIMULATE -> (T) new SimNode(CoreConfig.newInstance(SimConfig.class, properties), nodeServingTransport);
            default -> throw new IllegalArgumentException("Unknown protocol: " + nodeConfig.nodeConsensusProtocol());
        };
    }

    static Transport createTcpServerTransport(Properties properties) {
        return new VanillaTcpServerTransport(CoreConfig.newInstance(TcpTransportConfig.class, properties));
    }

    static Transport createTcpClientTransport(Properties properties) {
        return new VanillaTcpClientTransport(CoreConfig.newInstance(TcpTransportConfig.class, properties));
    }
}
