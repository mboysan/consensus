package com.mboysan.consensus;

public class BizurState {

    private int leaderId;
    private int electId;
    private int votedElectId;

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

    public BizurState setElectId(int electId) {
        this.electId = electId;
        return this;
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
}
