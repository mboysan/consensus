package com.mboysan.consensus;

public abstract class AggregateMeasurement extends Measurement {
    public AggregateMeasurement(String name, String value, long timestamp) {
        super(name, value, timestamp);
    }

    synchronized void doAggregate(String newValue) {
        final String newVal = aggregate(newValue);
        setValue(newVal);
        setTimestamp(System.currentTimeMillis());
    }

    public abstract String aggregate(String newValue);
}
