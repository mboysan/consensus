package com.mboysan.consensus.integration;

import com.mboysan.consensus.configuration.Destination;
import com.mboysan.consensus.configuration.TcpDestination;
import com.mboysan.consensus.util.NetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class TcpTransportCluster extends TransportCluster {
    private static final String HOST_NAME = "localhost";
    private static final List<TcpDestination> DESTINATIONS = new ArrayList<>();
    static {
        addDestination(0, NetUtil.findFreePort());
        addDestination(1, NetUtil.findFreePort());
        addDestination(2, NetUtil.findFreePort());
    }

    @Override
    List<? extends Destination> destinations() {
        return DESTINATIONS;
    }

    @Override
    Properties serverProperties(int serverId) {
        Properties properties = new Properties();
        properties.put("transport.tcp.server.port", String.valueOf(DESTINATIONS.get(serverId).port()));
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        properties.put("transport.tcp.server.socket.so_timeout", String.valueOf(2500)); // 2.5 seconds timeout
        return properties;
    }

    @Override
    Properties clientProperties() {
        Properties properties = new Properties();
        properties.put("transport.tcp.destinations", NetUtil.convertDestinationsListToProps(DESTINATIONS));
        return properties;
    }

    private static void addDestination(int nodeId, int port) {
        DESTINATIONS.add(new TcpDestination(nodeId, HOST_NAME, port));
    }
}
