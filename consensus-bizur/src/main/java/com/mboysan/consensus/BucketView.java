package com.mboysan.consensus;

import java.io.Serializable;
import java.util.Map;

public class BucketView implements Serializable, Comparable<BucketView> {

    private Map<String, String> bucketMap;
    private int index;

    private int verElectId;
    private int verCounter;

    private int leaderId;

    public Map<String, String> getBucketMap() {
        return bucketMap;
    }

    public BucketView setBucketMap(Map<String, String> bucketMap) {
        this.bucketMap = bucketMap;
        return this;
    }

    public int getIndex() {
        return index;
    }

    public BucketView setIndex(int index) {
        this.index = index;
        return this;
    }

    public int getVerElectId() {
        return verElectId;
    }

    public BucketView setVerElectId(int verElectId) {
        this.verElectId = verElectId;
        return this;
    }

    public int getVerCounter() {
        return verCounter;
    }

    public BucketView setVerCounter(int verCounter) {
        this.verCounter = verCounter;
        return this;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public BucketView setLeaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    @Override
    public int compareTo(BucketView o) {
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
        return "BucketView{" +
                "bucketMap=" + bucketMap +
                ", index=" + index +
                ", verElectId=" + verElectId +
                ", verCounter=" + verCounter +
                ", leaderId=" + leaderId +
                '}';
    }
}
