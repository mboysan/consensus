package com.mboysan.consensus.message;

public class CheckBizurIntegrityRequest extends RoutableRequest {

    private final int level;

    public CheckBizurIntegrityRequest(int level) {
        this(ROUTE_TO_SELF, level);
    }

    public CheckBizurIntegrityRequest(int routeTo, int level) {
        super(routeTo);
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public String toString() {
        return "CheckBizurIntegrityRequest{" +
                "level=" + level +
                '}' + super.toString();
    }
}
