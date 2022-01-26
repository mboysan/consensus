package com.mboysan.consensus.event;

public record NodeStartedEvent(int sourceNodeId) implements IEvent {
}
