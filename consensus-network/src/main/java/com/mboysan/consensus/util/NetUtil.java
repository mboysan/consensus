package com.mboysan.consensus.util;

import com.mboysan.consensus.configuration.Destination;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public final class NetUtil {

    private NetUtil(){}

    public static String convertDestinationsListToProps(List<Destination> destinations) {
        Objects.requireNonNull(destinations);
        StringJoiner sj = new StringJoiner(",");
        for (Destination destination : destinations) {
            sj.add(destination.toString());
        }
        return sj.toString();
    }

    public static Map<Integer, Destination> convertPropsToDestinationsMap(String destinationProps) {
        List<Destination> destinations = convertPropsToDestinationsList(destinationProps);
        return destinations.stream().collect(Collectors.toMap(Destination::nodeId, identity()));
    }

    public static List<Destination> convertPropsToDestinationsList(String destinationProps) {
        Objects.requireNonNull(destinationProps);
        List<Destination> destinations = new ArrayList<>();
        destinationProps = destinationProps.replaceAll("\\s+","");    // remove whitespace
        String[] dests = destinationProps.split(",");
        for (String dest : dests) {
            String[] idIp = dest.split("-");
            int id = Integer.parseInt(idIp[0]);
            String[] hostPort = idIp[1].split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);
            destinations.add(new Destination(id, host, port));
        }
        return destinations;
    }

    /**
     * Taken from: <a href="https://gist.github.com/vorburger/3429822">vorburger/gist:3429822</a>
     * Returns a free port number on localhost.
     * @return a free port number on localhost
     * @throws IllegalStateException if unable to find a free port
     */
    public static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not find a free TCP/IP port.", e);
        }
    }

}
