package com.mboysan.consensus;

public class Measurement implements Comparable<Measurement> {
    private String name;
    private String value;
    private long timestamp = System.currentTimeMillis();

    public Measurement(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Measurement setName(String name) {
        this.name = name;
        return this;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Measurement o) {
        if (timestamp < o.timestamp) {
            return -1;
        }
        return 1;
    }
}
