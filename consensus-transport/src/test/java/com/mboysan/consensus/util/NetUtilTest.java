package com.mboysan.consensus.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NetUtilTest {

    @Test
    void testConvertFail() {
        assertThrows(NullPointerException.class, () -> NetUtil.convertDestinationsListToProps(null));
        assertThrows(NullPointerException.class, () -> NetUtil.convertPropsToDestinationsMap(null));
        assertThrows(NullPointerException.class, () -> NetUtil.convertPropsToDestinationsList(null));

        assertThrows(IllegalArgumentException.class, () -> NetUtil.convertPropsToDestinationsMap(
                "some-incompatible-props"
        ));
    }

    @Test
    void testConvertPropsToListOk() {
        String destination = "0-127.0.0.1:8080";

        // test with back-and-forth conversion
        var c1 = NetUtil.convertPropsToDestinationsList(destination);
        var c2 = NetUtil.convertDestinationsListToProps(c1);
        assertEquals(destination, c2);

        destination += ",1-127.0.0.1:8081"; // add one more destination and retry
        c1 = NetUtil.convertPropsToDestinationsList(destination);
        c2 = NetUtil.convertDestinationsListToProps(c1);
        assertEquals(destination, c2);
    }

    @Test
    void testConvertPropsToDestinationsMapFail() {
        String destination = "0-127.0.0.1:8080";

        // test with back-and-forth conversion
        var c1 = NetUtil.convertPropsToDestinationsMap(destination);
        var c2 = NetUtil.convertDestinationsListToProps(new ArrayList<>(c1.values()));
        assertEquals(destination, c2);

        destination += ",1-127.0.0.1:8081"; // add one more destination and retry
        c1 = NetUtil.convertPropsToDestinationsMap(destination);
        c2 = NetUtil.convertDestinationsListToProps(new ArrayList<>(c1.values()));
        assertEquals(destination, c2);
    }
}