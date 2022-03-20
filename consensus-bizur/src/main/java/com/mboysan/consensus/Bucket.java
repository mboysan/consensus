package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Bucket implements Serializable, Comparable<Bucket> {

    private transient static final Logger LOGGER = LoggerFactory.getLogger(Bucket.class);

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

    void putOp(String key, String val) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("put key={},val={} in bucket={}", key, val, this);
        }
        bucketMap.put(key, val);
    }

    String getOp(String key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("get key={} from bucket={}", key, this);
        }
        return bucketMap.get(key);
    }

    void removeOp(String key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("remove key={} from bucket={}", key, this);
        }
        bucketMap.remove(key);
    }

    Set<String> getKeySetOp() {
        return bucketMap.keySet();
    }

    /*----------------------------------------------------------------------------------
     * Getters/Setters
     *----------------------------------------------------------------------------------*/

    void setBucketMap(Map<String, String> bucketMap) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("replacing bucketMap={} with map={} in bucket={}", this.bucketMap, bucketMap, this);
        }
        this.bucketMap = bucketMap;
    }

    public Map<String, String> getBucketMap() {
        return bucketMap;
    }

    int getVerElectId() {
        return verElectId;
    }

    void setVerElectId(int verElectId) {
        this.verElectId = verElectId;
    }

    int getVerCounter() {
        return verCounter;
    }

    void setVerCounter(int verCounter) {
        this.verCounter = verCounter;
    }

    void incrementVerCounter() {
        ++verCounter;
    }

    int getIndex() {
        return index;
    }

    /*----------------------------------------------------------------------------------
     * Utils
     *----------------------------------------------------------------------------------*/

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
