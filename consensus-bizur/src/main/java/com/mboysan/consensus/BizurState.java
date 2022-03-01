package com.mboysan.consensus;

class BizurState {

    private int leaderId = -1;
    private int electId = 0;
    private int votedElectId = -1;

    BizurState() {
        reset();
    }

    void reset() {
        leaderId = -1;
        electId = 0;
        votedElectId = -1;
    }

    int getLeaderId() {
        return leaderId;
    }

    BizurState setLeaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    int getElectId() {
        return electId;
    }

    int incrementAndGetElectId() {
        return ++this.electId;
    }

    int getVotedElectId() {
        return votedElectId;
    }

    BizurState setVotedElectId(int votedElectId) {
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
