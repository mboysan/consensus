package com.mboysan.consensus;

import org.aeonbits.owner.ConfigCache;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public interface NettyTransportConfig extends TransportConfig {

    @Key("transport.netty.port")
    int port();

    /**
     * Following is an example of supported TCP/IP destinations with node ids:
     * </br></br>
     * <code>
     *     0=localhost:8080, 1=localhost:8081, 2=localhost:8082
     * </code>
     * This example shows 3 nodes where node with id 0 is mapped to localhost on port 8080, node with id 1 is mapped to
     * localhost on port 8081 and so on...
     * @return map of nodeId and destination pairs.
     */
    @Key("transport.netty.destinations")
    @ConverterClass(DestinationsConverter.class)
    Map<Integer,String> destinations();

    @Key("transport.netty.clientPoolSize")
    @DefaultValue("8")
    int clientPoolSize();

    class DestinationsConverter implements Converter<Map<Integer, String>> {
        @Override
        public Map<Integer, String> convert(Method method, String s) {
            if (s == null) {
                return null;
            }
            s = s.replaceAll("\\s+","");    // remove whitespace
            Map<Integer,String> destinations = new HashMap<>();
            String[] allDestinations = s.split(",");
            for (String destination : allDestinations) {
                String[] idDestinationPair = destination.split("=");
                Integer id = Integer.parseInt(idDestinationPair[0]);
                String ipPortPair = idDestinationPair[1];
                destinations.put(id, ipPortPair);
            }
            return destinations;
        }
    }

    static NettyTransportConfig getCached(Properties... properties) {
        return ConfigCache.getOrCreate(NettyTransportConfig.class, properties);
    }
}
