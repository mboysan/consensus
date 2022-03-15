package com.mboysan.consensus.message;

import java.util.Set;

public class CollectKeysRequest extends Message {
    private final Set<Integer> rangeIndexes;

    public CollectKeysRequest(Set<Integer> rangeIndexes) {
        this.rangeIndexes = rangeIndexes;
    }

    public Set<Integer> rangeIndexes() {
        return rangeIndexes;
    }

    @Override
    public String toString() {
        return "CollectKeysRequest{" +
                "rangeIndexes=" + rangeIndexes +
                "} " + super.toString();
    }
}
