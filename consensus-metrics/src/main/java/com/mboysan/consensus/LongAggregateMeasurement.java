package com.mboysan.consensus;

public class LongAggregateMeasurement extends AggregateMeasurement {
    public LongAggregateMeasurement(String name, long value) {
        super(name, String.valueOf(value));
    }

    @Override
    public String aggregate(String newValue) {
        long prevValue = Long.parseLong(getValue());
        long newValueL = Long.parseLong(newValue);
        return String.valueOf(prevValue + newValueL);
    }
}
