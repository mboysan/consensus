package com.mboysan.consensus;

public abstract class AggregateMeasurement extends Measurement {
    public AggregateMeasurement(String name, String value) {
        super(name, value);
    }

    synchronized void doAggregate(String newValue) {
        final String newVal = aggregate(newValue);
        setValue(newVal);
        setTimestamp(System.currentTimeMillis());
    }

    public abstract String aggregate(String newValue);
}
