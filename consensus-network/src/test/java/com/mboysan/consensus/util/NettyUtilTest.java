package com.mboysan.consensus.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyUtilTest {

    @Test
    void testConvertFail() {
        assertThrows(NullPointerException.class, () -> NettyUtil.convertDestinationsListToProps(null));
        assertThrows(NullPointerException.class, () -> NettyUtil.convertPropsToDestinationsMap(null));
        assertThrows(NullPointerException.class, () -> NettyUtil.convertPropsToDestinationsList(null));

        assertThrows(IllegalArgumentException.class, () -> NettyUtil.convertPropsToDestinationsMap(
                "some-incompatible-props"
        ));
    }

    @Test
    void testConvertPropsToListOk() {
        String destination = "0=127.0.0.1:8080";

        // test with back-and-forth conversion
        var c1 = NettyUtil.convertPropsToDestinationsList(destination);
        var c2 = NettyUtil.convertDestinationsListToProps(c1);
        assertEquals(destination, c2);

        destination += ",1=127.0.0.1:8081"; // add one more destination and retry
        c1 = NettyUtil.convertPropsToDestinationsList(destination);
        c2 = NettyUtil.convertDestinationsListToProps(c1);
        assertEquals(destination, c2);
    }

    @Test
    void testConvertPropsToDestinationsMapFail() {
        String destination = "0=127.0.0.1:8080";

        // test with back-and-forth conversion
        var c1 = NettyUtil.convertPropsToDestinationsMap(destination);
        var c2 = NettyUtil.convertDestinationsListToProps(new ArrayList<>(c1.values()));
        assertEquals(destination, c2);

        destination += ",1=127.0.0.1:8081"; // add one more destination and retry
        c1 = NettyUtil.convertPropsToDestinationsMap(destination);
        c2 = NettyUtil.convertDestinationsListToProps(new ArrayList<>(c1.values()));
        assertEquals(destination, c2);
    }
}