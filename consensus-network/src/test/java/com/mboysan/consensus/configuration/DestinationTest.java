package com.mboysan.consensus.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DestinationTest {

    @Test
    void testDestinationCreateFail() {
        assertThrows(NullPointerException.class, () -> new Destination(0, null, 8080));
        assertThrows(IllegalArgumentException.class, () -> new Destination(0, "???", 8080));
    }

    @Test
    void testDestinationCreateOK() {
        String expected = "0=127.0.0.1:8080";
        Destination actual;

        actual = new Destination(0, "localhost", 8080);
        assertEquals(expected, actual.toString());
        actual = new Destination(0, "127.0.0.1", 8080);
        assertEquals(expected, actual.toString());
    }
}