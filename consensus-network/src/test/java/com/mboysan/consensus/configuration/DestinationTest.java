package com.mboysan.consensus.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DestinationTest {

    @Test
    void testDestinationNotEquals() {
        Destination d1 = new Destination(0, "127.0.0.0", 8080);
        Destination d2 = new Destination(0, "127.0.0.1", 8081);
        Destination d3 = new Destination(0, "127.0.0.2", 8082);

        assertNotEquals(d1, d2);
        assertNotEquals(d1.hashCode(), d2.hashCode());
        assertNotEquals(d1, d3);
        assertNotEquals(d1.hashCode(), d3.hashCode());
        assertNotEquals(d2, d3);
        assertNotEquals(d2.hashCode(), d3.hashCode());
    }

    @Test
    void testDestinationEquals() {
        Destination d1 = new Destination(0, "127.0.0.0", 8080);
        Destination d2 = new Destination(0, "127.0.0.0", 8080);

        assertEquals(d1, d2);
        assertEquals(d1.hashCode(), d2.hashCode());
    }
}