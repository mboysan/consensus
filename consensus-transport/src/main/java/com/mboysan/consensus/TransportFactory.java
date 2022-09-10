package com.mboysan.consensus;

import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.configuration.TransportConfig;

import java.util.Properties;

public final class TransportFactory {
    private TransportFactory() {}

    public static <T extends Transport> T createClientTransport(Properties properties) {
        TransportConfig baseConf = CoreConfig.newInstance(TransportConfig.class, properties);
        if ("invm".equals(baseConf.type())) {
            //TODO
            return null;
        } else {
            // tcp
            TcpTransportConfig tcpConfig = CoreConfig.newInstance(TcpTransportConfig.class, properties);
            return createClientTransport(tcpConfig);
        }
    }

    public static <T extends Transport> T createClientTransport(TransportConfig config) {
        return createClientTransport(config, -1);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Transport> T createClientTransport(TransportConfig config, int associatedServerId) {
        ClientTransport clientTransport = new ClientTransport(config, associatedServerId);
        return (T) clientTransport;
    }

    public static <T extends Transport> T createServerTransport(Properties properties) {
        TransportConfig baseConf = CoreConfig.newInstance(TransportConfig.class, properties);
        if ("invm".equals(baseConf.type())) {
            //TODO
            return null;
        } else {
            // tcp
            TcpTransportConfig tcpConfig = CoreConfig.newInstance(TcpTransportConfig.class, properties);
            return createServerTransport(tcpConfig);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Transport> T createServerTransport(TransportConfig config) {
        if (config instanceof TcpTransportConfig conf) {
            return (T) new ServerTransport(conf);
        } else {
            // TODO:
            return null;
        }
    }
}
