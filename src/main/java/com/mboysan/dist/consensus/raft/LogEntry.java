package com.mboysan.dist.consensus.raft;

public class LogEntry {

    private final String command;
    private final int term;

    public LogEntry(String command, int term) {
        this.command = command;
        this.term = term;
    }

    public String getCommand() {
        return command;
    }

    public int getTerm() {
        return term;
    }
}
