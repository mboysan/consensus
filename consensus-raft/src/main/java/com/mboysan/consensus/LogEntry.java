package com.mboysan.consensus;

import java.io.Serializable;
import java.util.Objects;

class LogEntry implements Serializable {

    private final String command;
    private final int term;

    LogEntry(String command, int term) {
        this.command = command;
        this.term = term;
    }

    String getCommand() {
        return command;
    }

    int getTerm() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry)) return false;

        LogEntry logEntry = (LogEntry) o;

        if (term != logEntry.term) return false;
        return Objects.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        int result = command != null ? command.hashCode() : 0;
        result = 31 * result + term;
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "command='" + command + '\'' +
                ", term=" + term +
                '}';
    }
}
