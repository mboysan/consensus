package com.mboysan.consensus.configuration;

public record TcpDestination(int nodeId, String ip, int port) implements Destination {
    @Override
    public String toString() {
        return nodeId + "-" + ip + ":" + port;
    }
}
