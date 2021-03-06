package com.mboysan.consensus.configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TcpTransportConfigTest {

    @Test
    void testDestinationConverterSuccess() {
        Properties props = new Properties();
        props.put("transport.tcp.destinations", "0-localhost:8080,1-localhost:8081, 2-localhost:8082");
        TcpTransportConfig config = CoreConfig.newInstance(TcpTransportConfig.class, props);

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
        props.put("transport.tcp.destinations", "invalid");
        var config = CoreConfig.newInstance(TcpTransportConfig.class, props);
        assertThrows(RuntimeException.class, config::destinations);
    }
}