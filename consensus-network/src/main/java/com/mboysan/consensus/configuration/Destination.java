package com.mboysan.consensus.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Destination {
    private final int nodeId;
    private final InetAddress ip;
    private final int port;

    public Destination(int nodeId, String ip, int port) {
        this.nodeId = nodeId;
        try {
            this.ip = InetAddress.getByName(Objects.requireNonNull(ip));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
        this.port = port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public InetAddress getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Destination that)) return false;
        if (nodeId != that.nodeId) return false;
        if (port != that.port) return false;
        return ip.equals(that.ip);
    }

    @Override
    public int hashCode() {
        int result = nodeId;
        result = 31 * result + ip.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return nodeId + "=" + ip.getHostAddress() + ":" + port;
    }
}
