package com.mboysan.consensus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class BucketTest {

    @Test
    void testBucketsAreEqual() {
        Bucket bucket1 = new Bucket(0);
        bucket1.setVerElectId(123);
        bucket1.setVerCounter(321);
        bucket1.getBucketMap().put("key1", "value1");
        bucket1.getBucketMap().put("key2", "value2");

        Bucket bucket2 = new Bucket(0);
        bucket2.setVerElectId(123);
        bucket2.setVerCounter(321);
        bucket2.getBucketMap().put("key2", "value2");
        bucket2.getBucketMap().put("key1", "value1");

        assertEquals(bucket1, bucket2);
    }

    @Test
    void testBucketsAreNotEqual() {
        Bucket bucket1 = new Bucket(0);
        Bucket bucket2 = new Bucket(0);

        bucket1.setVerElectId(1);
        bucket2.setVerElectId(2);
        assertNotEquals(bucket1, bucket2);

        bucket1.setVerElectId(1);
        bucket2.setVerElectId(1);
        bucket1.setVerCounter(1);
        bucket2.setVerCounter(2);
        assertNotEquals(bucket1, bucket2);

        bucket1.setVerElectId(1);
        bucket2.setVerElectId(1);
        bucket1.setVerCounter(1);
        bucket2.setVerCounter(1);
        bucket1.getBucketMap().put("key1", "value1");
        assertNotEquals(bucket1, bucket2);
    }

}
