package com.mboysan.consensus.event;

public record NodeStoppedEvent(int sourceNodeId) implements IEvent {
}
