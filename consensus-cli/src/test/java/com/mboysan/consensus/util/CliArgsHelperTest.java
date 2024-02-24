package com.mboysan.consensus.util;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CliArgsHelperTest {

    @Test
    void test_getProperties() {
        String[] args = new String[] {"propKey1=propVal1", "propKey2=propVal2"};
        Properties actual = CliArgsHelper.getProperties(args);
        assertEquals("propVal1", actual.getProperty("propKey1"));
        assertEquals("propVal2", actual.getProperty("propKey2"));
    }

    @Test
    void test_getProperties_alias_map_works() {
        CliArgsHelper.ALIAS_MAP.keySet().forEach(key -> {
            String expected = "some-value";
            String[] args = new String[] {key + "=" + expected};
            Properties actual = CliArgsHelper.getProperties(args);
            String realKey = CliArgsHelper.ALIAS_MAP.get(key);
            assertEquals(actual.getProperty(realKey), expected);
        });
    }

    @Test
    void test_getNodeSectionProperties() {
        String[] args = new String[] {
                "--store", "key1=store_value1", "key2=store_value2",
                "--node", "key1=node_value1", "key2=node_value2",
                "--store", "key3=store_value3", "key1=overridden_store_value1",
                "--node", "key3=node_value3", "key1=overridden_node_value1",
        };
        Properties actual = CliArgsHelper.getNodeSectionProperties(args);
        assertEquals("overridden_node_value1", actual.getProperty("key1"));
        assertEquals("node_value2", actual.getProperty("key2"));
        assertEquals("node_value3", actual.getProperty("key3"));
    }

    @Test
    void test_getStoreSectionProperties() {
        String[] args = new String[] {
                "--store", "key1=store_value1", "key2=store_value2",
                "--node", "key1=node_value1", "key2=node_value2",
                "--store", "key3=store_value3", "key1=overridden_store_value1",
                "--node", "key3=node_value3", "key1=overridden_node_value1",
                "--node", "key4=node_value4", "key5=node_value5",
        };
        Properties actual = CliArgsHelper.getStoreSectionProperties(args);
        assertEquals("overridden_store_value1", actual.getProperty("key1"));
        assertEquals("store_value2", actual.getProperty("key2"));
        assertEquals("store_value3", actual.getProperty("key3"));
        assertEquals("node_value4", actual.getProperty("key4"));
        assertEquals("node_value5", actual.getProperty("key5"));
    }
}