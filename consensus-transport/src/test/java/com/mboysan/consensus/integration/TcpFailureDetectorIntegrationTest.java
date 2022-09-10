package com.mboysan.consensus.integration;

import com.mboysan.consensus.Transport;

import java.util.Properties;

class TcpFailureDetectorIntegrationTest extends FailureDetectorIntegrationTestBase {

    private final TransportCluster transportCluster = new TcpTransportCluster();

    @Override
    Properties clientProperties() {
        return transportCluster.clientProperties();
    }

    @Override
    Transport createClientTransport(Properties properties) {
        return transportCluster.createClientTransport(properties);
    }

    @Override
    Transport createServerTransport(int serverId) {
        return transportCluster.createServerTransport(serverId);
    }
}
