package com.mboysan.consensus;

public class BizurState {

    private int leaderId = -1;
    private int electId = 0;
    private int votedElectId = 0;

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
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

    public void setVotedElectId(int votedElectId) {
        this.votedElectId = votedElectId;
    }
}
