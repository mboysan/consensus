package com.mboysan.consensus.integration;

import com.mboysan.consensus.Transport;

import java.io.IOException;

class TcpTransportIntegrationTest extends TransportIntegrationTestBase {

    private final TransportCluster transportCluster = new TcpTransportCluster();

    @Override
    void setUp() throws IOException {
        transportCluster.setUp();
    }

    @Override
    void tearDown() {
        transportCluster.tearDown();
    }

    @Override
    Transport createClientTransport() {
        return transportCluster.createClientTransport();
    }

    @Override
    Transport[] getServerTransports() {
        return transportCluster.serverTransports();
    }

    @Override
    Transport[] getClientTransports() {
        return transportCluster.clientTransports();
    }
}
