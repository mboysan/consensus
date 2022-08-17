package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoRPCProtocol;
import com.mboysan.consensus.configuration.CoreConfig;
import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import com.mboysan.consensus.util.NetUtil;
import com.mboysan.consensus.vanilla.VanillaTcpClientTransport;
import com.mboysan.consensus.vanilla.VanillaTcpServerTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

class VanillaTcpTransportTestBase {
    private static final String HOST_NAME = "localhost";

    private static final int NUM_SERVERS = 3;
    private static final int NUM_CLIENTS = 3;
    private static final List<Destination> DESTINATIONS = new ArrayList<>();
    static {
        addDestination(0, NetUtil.findFreePort());
        addDestination(1, NetUtil.findFreePort());
        addDestination(2, NetUtil.findFreePort());
    }

    VanillaTcpServerTransport[] setupServers() {
        var serverTransports = new VanillaTcpServerTransport[NUM_SERVERS];
        for (int i = 0; i < serverTransports.length; i++) {
            VanillaTcpServerTransport serverTransport = createServerTransport(i);
            serverTransport.registerMessageProcessor(new EchoRPCProtocol());
            serverTransports[i] = serverTransport;
            serverTransport.start();
        }
        return serverTransports;
    }

    VanillaTcpClientTransport[] setupClients() {
        var clientTransports = new VanillaTcpClientTransport[NUM_CLIENTS];
        for (int i = 0; i < clientTransports.length; i++) {
            VanillaTcpClientTransport clientTransport = createClientTransport();
            clientTransports[i] = clientTransport;
            clientTransport.start();
        }
        return clientTransports;
    }

    void teardownServers(VanillaTcpServerTransport[] serverTransports) {
        for (VanillaTcpServerTransport serverTransport : serverTransports) {
            serverTransport.shutdown();
            assertTrue(serverTransport.verifyShutdown());
        }
    }

    void teardownClients(VanillaTcpClientTransport[] clientTransports) {
        for (VanillaTcpClientTransport clientTransport : clientTransports) {
            clientTransport.shutdown();
            assertTrue(clientTransport.verifyShutdown());
        }
    }

    VanillaTcpServerTransport createServerTransport(int serverId) {
        return createServerTransport(serverProperties(serverId));
    }

    VanillaTcpServerTransport createServerTransport(Properties serverProperties) {
        // create new config per transport
        TcpTransportConfig config = CoreConfig.newInstance(TcpTransportConfig.class, serverProperties);
        return new VanillaTcpServerTransport(config);
    }

    Properties serverProperties(int serverId) {
        Properties properties = new Properties();
        properties.put("transport.tcp.server.port", String.valueOf(DESTINATIONS.get(serverId).port()));
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        properties.put("transport.tcp.server.socket.so_timeout", String.valueOf(2500)); // 2.5 seconds timeout
        return properties;
    }

    VanillaTcpClientTransport createClientTransport() {
        return createClientTransport(clientProperties());
    }

    VanillaTcpClientTransport createClientTransport(Properties clientProperties) {
        // create new config per transport
        TcpTransportConfig config = CoreConfig.newInstance(TcpTransportConfig.class, clientProperties);
        return new VanillaTcpClientTransport(config);
    }

    Properties clientProperties() {
        Properties properties = new Properties();
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        return properties;
    }

    private static void addDestination(int nodeId, int port) {
        DESTINATIONS.add(new Destination(nodeId, HOST_NAME, port));
    }
}
