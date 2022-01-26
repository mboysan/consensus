package com.mboysan.consensus.event;

import java.util.Set;

public record NodeListChangedEvent(int targetNodeId, Set<Integer> serverIds) implements IEvent {
}
