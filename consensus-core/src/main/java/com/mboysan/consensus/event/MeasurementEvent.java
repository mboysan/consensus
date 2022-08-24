package com.mboysan.consensus.event;

public class MeasurementEvent implements IEvent {

    private final MeasurementType measurementType;
    private final String name;
    private final Object payload;
    private final long timestamp = System.currentTimeMillis();

    public MeasurementEvent(MeasurementType measurementType, String name, Object payload) {
        this.measurementType = measurementType;
        this.name = name;
        this.payload = payload;
    }

    public MeasurementType getMeasurementType() {
        return measurementType;
    }

    public String getName() {
        return name;
    }

    public Object getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public enum MeasurementType {
        SAMPLE, AGGREGATE
    }
}
