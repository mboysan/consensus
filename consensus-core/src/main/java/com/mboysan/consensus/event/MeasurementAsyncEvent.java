package com.mboysan.consensus.event;

public class MeasurementAsyncEvent extends MeasurementEvent {
    public MeasurementAsyncEvent(MeasurementType measurementType, String name, Object payload) {
        super(measurementType, name, payload);
    }
}
