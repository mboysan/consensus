package com.mboysan.consensus.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HashUtilTest {

    @Test
    void testFindCommonHashWithInvalidThreshold() {
        assertThrows(IllegalArgumentException.class, () -> HashUtil.findCommonHash(List.of(), 1));
        assertThrows(IllegalArgumentException.class, () -> HashUtil.findCommonHash(List.of("a"), -1));
    }

    @Test
    void testFindCommonHashWithValidThreshold() {
        String expectedHash = "hash1";
        List<String> hashes = new ArrayList<>();
        hashes.add(expectedHash);
        hashes.add("hash2");
        hashes.add(expectedHash);
        hashes.add("hash3");
        hashes.add(expectedHash);

        Optional<String> actualHash = HashUtil.findCommonHash(hashes, 3);
        assertEquals(expectedHash, actualHash.orElseThrow());
    }

    @Test
    void testFindCommonHashWithValidThresholdReturningEmptyOptional() {
        List<String> hashes = new ArrayList<>();
        hashes.add("hash1");
        hashes.add("hash2");
        hashes.add("hash1");
        hashes.add("hash3");
        hashes.add("hash1");

        Optional<String> actualHash = HashUtil.findCommonHash(hashes, 4);
        assertTrue(actualHash.isEmpty());
    }

}