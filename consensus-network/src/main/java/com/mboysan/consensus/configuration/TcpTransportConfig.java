package com.mboysan.consensus.configuration;

import com.mboysan.consensus.util.NetUtil;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Map;

public interface TcpTransportConfig extends CoreConfig {

    interface Param {
        String MESSAGE_CALLBACK_TIMEOUT_MS = "transport.message.callbackTimeoutMs";
        String CLIENT_POOL_SIZE = "transport.tcp.clientPoolSize";
        String SERVER_PORT = "transport.tcp.server.port";
        String SOCKET_SO_TIMEOUT = "transport.tcp.server.socket.so_timeout";
        String MARK_SERVER_AS_FAILED_COUNT = "transport.tcp.client.failure.markServerAsFailedCount";
        String PING_INTERVAL = "transport.tcp.client.failure.pingInterval";
        String DESTINATIONS = "transport.tcp.destinations";
    }

    @Key(Param.MESSAGE_CALLBACK_TIMEOUT_MS)
    @DefaultValue("5000")
    long messageCallbackTimeoutMs();

    @Key(Param.CLIENT_POOL_SIZE)
    @DefaultValue("-1")
    int clientPoolSize();

    @Key(Param.SERVER_PORT)
    int port();

    @Key(Param.SOCKET_SO_TIMEOUT)
    @DefaultValue("0")  // no timeout
    int socketSoTimeout();

    @Key(Param.MARK_SERVER_AS_FAILED_COUNT)
    @DefaultValue("3")
    int markServerAsFailedCount();

    @Key(Param.PING_INTERVAL)
    @DefaultValue("5000")   // 5 seconds
    long pingInterval();

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
    @Key(Param.DESTINATIONS)
    @ConverterClass(DestinationsConverter.class)
    Map<Integer, TcpDestination> destinations();

    class DestinationsConverter implements Converter<Map<Integer, TcpDestination>> {
        @Override
        public Map<Integer, TcpDestination> convert(Method method, String s) {
            return NetUtil.convertPropsToDestinationsMap(s);
        }
    }
}
