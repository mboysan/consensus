package com.mboysan.consensus.integration;

import com.mboysan.consensus.EchoProtocolImpl;
import com.mboysan.consensus.Transport;
import com.mboysan.consensus.TransportFactory;
import com.mboysan.consensus.configuration.Destination;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public abstract class TransportCluster {
    private Transport[] serverTransports;
    private Transport[] clientTransports;

    void setUp() throws IOException {
        this.serverTransports = setupServers();
        this.clientTransports = setupClients();
    }

    void tearDown() {
        teardown(serverTransports);
        teardown(clientTransports);
    }

    abstract List<? extends Destination> destinations();

    Transport[] setupServers() throws IOException {
        var serverTransports = new Transport[destinations().size()];
        for (int i = 0; i < serverTransports.length; i++) {
            Transport serverTransport = createServerTransport(i);
            serverTransport.registerMessageProcessor(new EchoProtocolImpl());
            serverTransports[i] = serverTransport;
            serverTransport.start();
        }
        return serverTransports;
    }

    Transport[] setupClients() throws IOException {
        var clientTransports = new Transport[destinations().size()];
        for (int i = 0; i < clientTransports.length; i++) {
            Transport clientTransport = createClientTransport();
            clientTransports[i] = clientTransport;
            clientTransport.start();
        }
        return clientTransports;
    }

    void teardown(Transport[] serverTransports) {
        for (Transport serverTransport : serverTransports) {
            serverTransport.shutdown();
        }
    }

    Transport createServerTransport(int serverId) {
        return createServerTransport(serverProperties(serverId));
    }

    Transport createServerTransport(Properties serverProperties) {
        return TransportFactory.createServerTransport(serverProperties);
    }

    abstract Properties serverProperties(int serverId);

    Transport createClientTransport() {
        return createClientTransport(clientProperties());
    }

    Transport createClientTransport(Properties clientProperties) {
        return TransportFactory.createClientTransport(clientProperties);
    }

    abstract Properties clientProperties();

    public Transport[] serverTransports() {
        return serverTransports;
    }

    public Transport[] clientTransports() {
        return clientTransports;
    }
}
