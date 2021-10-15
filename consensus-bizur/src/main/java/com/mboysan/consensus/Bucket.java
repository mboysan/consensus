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

    public String putOp(String key, String val) {
        LOGGER.trace("put key={},val={} in bucket={}", key, val, this);
        return bucketMap.put(key, val);
    }

    public String getOp(String key) {
        LOGGER.trace("get key={} from bucket={}", key, this);
        return bucketMap.get(key);
    }

    public String removeOp(String key) {
        LOGGER.trace("remove key={} from bucket={}", key, this);
        return bucketMap.remove(key);
    }

    public Set<String> getKeySetOp() {
        return bucketMap.keySet();
    }

    /*----------------------------------------------------------------------------------
     * Getters/Setters
     *----------------------------------------------------------------------------------*/

    public Bucket setBucketMap(Map<String, String> bucketMap) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("replacing bucketMap={} with map={} in bucket={}", this.bucketMap, bucketMap, this);
        }
        this.bucketMap = bucketMap;
        return this;
    }

    public int getVerElectId() {
        return verElectId;
    }

    public Bucket setVerElectId(int verElectId) {
        this.verElectId = verElectId;
        return this;
    }

    public int getVerCounter() {
        return verCounter;
    }

    public Bucket setVerCounter(int verCounter) {
        this.verCounter = verCounter;
        return this;
    }

    public void incrementVerCounter() {
        ++verCounter;
    }

    public int getIndex() {
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

    boolean isLocked() {
        return bucketLock.isLocked();
    }

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
