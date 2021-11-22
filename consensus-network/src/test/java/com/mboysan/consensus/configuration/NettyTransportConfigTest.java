package com.mboysan.consensus.configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyTransportConfigTest {

    @Test
    void testDestinationConverterSuccess() {
        Properties props = new Properties();
        props.put("transport.netty.destinations", "0=localhost:8080,1=localhost:8081, 2=localhost:8082");
        NettyTransportConfig config = Configuration.newInstance(NettyTransportConfig.class, props);

        Map<Integer, Destination> expected = new HashMap<>() {{
            put(0, new Destination(0, "localhost", 8080));
            put(1, new Destination(1, "localhost", 8081));
            put(2, new Destination(2, "localhost", 8082));
        }};
        Map<Integer, Destination> actual = config.destinations();

        assertEquals(expected, actual);
    }

    @Test
    void testDestinationConverterFails() {
        Properties props = new Properties();
        props.put("transport.netty.destinations", "invalid");
        var config = Configuration.newInstance(NettyTransportConfig.class, props);
        assertThrows(RuntimeException.class, config::destinations);
    }
}