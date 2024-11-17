package com.mboysan.consensus;

import com.mboysan.consensus.time.Timestamp;

import java.util.Objects;

public class Measurement implements Comparable<Measurement> {
    private String name;
    private String value;
    private Timestamp timestamp;

    public Measurement(String name, String value, Timestamp timestamp) {
        this.name = Objects.requireNonNull(name);
        this.value = Objects.requireNonNull(value);
        this.timestamp = Objects.requireNonNull(timestamp);
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
        return timestamp.getTimestamp();
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Measurement o) {
        if (timestamp.getTimestamp() <= o.timestamp.getTimestamp()) {
            return -1;
        }
        return 1;
    }
}
