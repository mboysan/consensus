package com.mboysan.consensus.message;

public class CheckStoreIntegrityRequest extends RoutableRequest {

    private final int level;

    public CheckStoreIntegrityRequest(int routeTo, int level) {
        super(routeTo);
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "CheckStoreIntegrityRequest{" +
                "level=" + level +
                "} " + super.toString();
    }
}
