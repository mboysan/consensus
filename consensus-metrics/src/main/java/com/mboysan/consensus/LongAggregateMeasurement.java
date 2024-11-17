package com.mboysan.consensus;

import com.mboysan.consensus.time.Timestamp;

public class LongAggregateMeasurement extends AggregateMeasurement {
    public LongAggregateMeasurement(String name, String value, Timestamp timestamp) {
        super(name, value, timestamp);
    }

    @Override
    public String aggregate(String newValue) {
        long prevValue = Long.parseLong(getValue());
        long newValueL = Long.parseLong(newValue);
        return String.valueOf(prevValue + newValueL);
    }
}
