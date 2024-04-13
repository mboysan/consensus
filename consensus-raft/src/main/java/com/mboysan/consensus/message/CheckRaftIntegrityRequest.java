package com.mboysan.consensus.message;

public class CheckRaftIntegrityRequest extends RoutableRequest {

    private final int level;

    public CheckRaftIntegrityRequest(int level) {
        this(ROUTE_TO_SELF, level);
    }

    public CheckRaftIntegrityRequest(int routeTo, int level) {
        super(routeTo);
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "CheckRaftIntegrityRequest{" +
                "level=" + level +
                '}' + super.toString();
    }
}
