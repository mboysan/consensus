package com.mboysan.consensus.message;

public class CheckRaftIntegrityRequest extends Message {

    public interface Level {
        int STATE = 1;
        int THIN_STATE = 2;
        int STATE_FROM_ALL = 3;
        int THIN_STATE_FROM_ALL = 4;
    }

    private final int level;

    public CheckRaftIntegrityRequest(int level) {
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
