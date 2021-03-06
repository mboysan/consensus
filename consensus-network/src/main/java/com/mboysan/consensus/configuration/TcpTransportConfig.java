package com.mboysan.consensus.configuration;

import com.mboysan.consensus.util.NetUtil;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Map;

public interface TcpTransportConfig extends TransportConfig {

    @Key("transport.tcp.clientPoolSize")
    @DefaultValue("-1")
    int clientPoolSize();

    @Key("transport.tcp.server.port")
    int port();

    /**
     * Following is an example of supported TCP/IP destinations with node ids:
     * </br></br>
     * <code>
     *     0-localhost:8080, 1-localhost:8081, 2-localhost:8082
     * </code>
     * This example shows 3 nodes where node with id 0 is mapped to localhost on port 8080, node with id 1 is mapped to
     * localhost on port 8081 and so on...
     * @return map of nodeId and destination pairs.
     */
    @Key("transport.tcp.destinations")
    @ConverterClass(DestinationsConverter.class)
    Map<Integer, Destination> destinations();

    class DestinationsConverter implements Converter<Map<Integer, Destination>> {
        @Override
        public Map<Integer, Destination> convert(Method method, String s) {
            return NetUtil.convertPropsToDestinationsMap(s);
        }
    }
}
