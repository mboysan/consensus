package com.mboysan.consensus.configuration;

public record Destination(int nodeId, String ip, int port) {
    @Override
    public String toString() {
        return nodeId + "-" + ip + ":" + port;
    }
}
