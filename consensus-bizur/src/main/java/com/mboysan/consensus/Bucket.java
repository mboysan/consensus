package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

class Bucket implements Comparable<Bucket> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bucket.class);

    private final ReentrantLock bucketLock = new ReentrantLock();

    private final int index;

    private int verElectId = 0;
    private int verCounter = 0;

    private Map<String, String> bucketMap = new HashMap<>();

    Bucket(int index) {
        this.index = index;
    }

    /*----------------------------------------------------------------------------------
     * Map Operations
     *----------------------------------------------------------------------------------*/

    String putOp(String key, String val) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("put key={},val={} in bucket={}", key, val, this);
        }
        return bucketMap.put(key, val);
    }

    String getOp(String key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("get key={} from bucket={}", key, this);
        }
        return bucketMap.get(key);
    }

    String removeOp(String key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("remove key={} from bucket={}", key, this);
        }
        return bucketMap.remove(key);
    }

    Set<String> getKeySetOp() {
        return bucketMap.keySet();
    }

    /*----------------------------------------------------------------------------------
     * Getters/Setters
     *----------------------------------------------------------------------------------*/

    Bucket setBucketMap(Map<String, String> bucketMap) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("replacing bucketMap={} with map={} in bucket={}", this.bucketMap, bucketMap, this);
        }
        this.bucketMap = bucketMap;
        return this;
    }

    int getVerElectId() {
        return verElectId;
    }

    Bucket setVerElectId(int verElectId) {
        this.verElectId = verElectId;
        return this;
    }

    int getVerCounter() {
        return verCounter;
    }

    Bucket setVerCounter(int verCounter) {
        this.verCounter = verCounter;
        return this;
    }

    void incrementVerCounter() {
        ++verCounter;
    }

    int getIndex() {
        return index;
    }

    /*----------------------------------------------------------------------------------
     * Bucket View
     *----------------------------------------------------------------------------------*/
    BucketView createView() {
        return new BucketView()
                .setBucketMap(new HashMap<>(bucketMap))
                .setIndex(getIndex())
                .setVerElectId(getVerElectId())
                .setVerCounter(getVerCounter());
    }

    /*----------------------------------------------------------------------------------
     * Utils
     *----------------------------------------------------------------------------------*/

    Bucket lock() {
        bucketLock.lock();
        return this;
    }

    void unlock() {
        bucketLock.unlock();
    }

    @Override
    public int compareTo(Bucket o) {
        if (this.getVerElectId() > o.getVerElectId()) {
            return 1;
        } else if (this.getVerElectId() == o.getVerElectId()) {
            return Integer.compare(this.getVerCounter(), o.getVerCounter());
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "index=" + index +
                ", verElectId=" + verElectId +
                ", verCounter=" + verCounter +
                ", bucketMap=" + bucketMap +
                '}';
    }
}
