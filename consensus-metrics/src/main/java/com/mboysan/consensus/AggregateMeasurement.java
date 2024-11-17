package com.mboysan.consensus;

import com.mboysan.consensus.time.Timestamp;
import com.mboysan.consensus.time.UnixSecondsTimestamp;

public abstract class AggregateMeasurement extends Measurement {
    AggregateMeasurement(String name, String value, Timestamp timestamp) {
        super(name, value, timestamp);
    }

    synchronized void doAggregate(String newValue) {
        final String newVal = aggregate(newValue);
        setValue(newVal);
        setTimestamp(new UnixSecondsTimestamp());
    }

    public abstract String aggregate(String newValue);
}
