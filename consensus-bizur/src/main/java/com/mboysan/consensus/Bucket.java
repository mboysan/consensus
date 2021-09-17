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

    private int leaderId;

    private int electId = 0;
    private int votedElectId = 0;

    private boolean seenLeader = false;

    private Map<String, String> bucketMap = new HashMap<>();
    private int verElectId = 0;
    private int verCounter = 0;

    private final int index;

    Bucket(int index) {
        this.index = index;
    }

    /*----------------------------------------------------------------------------------
     * Map Operations
     *----------------------------------------------------------------------------------*/

    public String putOp(String key, String val) {
        LOGGER.debug("put key={},val={} in bucket={}", key, val, this);
        return bucketMap.put(key, val);
    }

    public String getOp(String key) {
        LOGGER.debug("get key={} from bucket={}", key, this);
        return bucketMap.get(key);
    }

    public String removeOp(String key) {
        LOGGER.debug("remove key={} from bucket={}", key, this);
        return bucketMap.remove(key);
    }

    public Set<String> getKeySetOp() {
        return bucketMap.keySet();
    }

    /*----------------------------------------------------------------------------------
     * Getters/Setters
     *----------------------------------------------------------------------------------*/

    public Bucket setBucketMap(Map<String, String> bucketMap) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("replacing bucketMap={} with map={} in bucket={}", this.bucketMap, bucketMap, this);
        }
        this.bucketMap = bucketMap;
        return this;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public Bucket setLeaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int incrementAndGetElectId() {
        return ++electId;
    }

    public int getElectId() {
        return electId;
    }

    public int getVotedElectId() {
        return votedElectId;
    }

    public Bucket setVotedElectId(int votedElectId) {
        this.votedElectId = votedElectId;
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

    public int incrementAndGetVerCounter() {
        return ++verCounter;
    }

    public boolean seenLeader() {
        return seenLeader;
    }

    public Bucket setSeenLeader(boolean seenLeader) {
        this.seenLeader = seenLeader;
        return this;
    }

    public int getIndex() {
        return index;
    }

    /*----------------------------------------------------------------------------------
     * Bucket View
     *----------------------------------------------------------------------------------*/
    BucketView createView() {
        return new BucketView()
                .setBucketMap(Map.copyOf(bucketMap))
                .setIndex(getIndex())
                .setVerElectId(getVerElectId())
                .setVerCounter(getVerCounter())
                .setLeaderId(getLeaderId());
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
}
