package com.mboysan.consensus;

public class BizurState {

    private int leaderId = -1;
    private int electId = 0;
    private int votedElectId = -1;

    public int getLeaderId() {
        return leaderId;
    }

    public BizurState setLeaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int getElectId() {
        return electId;
    }

    public int incrementAndGetElectId() {
        return ++this.electId;
    }

    public int getVotedElectId() {
        return votedElectId;
    }

    public BizurState setVotedElectId(int votedElectId) {
        this.votedElectId = votedElectId;
        return this;
    }

    @Override
    public String toString() {
        return "BizurStateMutable{" +
                "leaderId=" + leaderId +
                ", electId=" + electId +
                ", votedElectId=" + votedElectId +
                '}';
    }
}
