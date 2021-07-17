package com.mboysan.consensus;

import java.util.ArrayList;
import java.util.List;

/**
 * Index of the log entries start from 0 unlike described in Raft paper which starts from 1.
 */
public class RaftLog implements Comparable<RaftLog> {
    private final List<LogEntry> entries = new ArrayList<>();

    int lastLogIndex() {
        return entries.size() - 1;
    }

    int lastLogTerm() {
        return logTerm(lastLogIndex());
    }

    int logTerm(int index) {
        if (index < 0 || index >= entries.size()) {
            return 0;
        }
        return get(index).getTerm();
    }

    void push(LogEntry entry) {
        if (entry.getTerm() == 0 || entry.getTerm() < lastLogTerm()) {
            throw new IllegalArgumentException("new entry has lower term than the last entry's term");
        }
        entries.add(entry);
    }

    LogEntry pop() {
        return entries.remove(entries.size() - 1);
    }

    LogEntry get(int index) {
        return entries.get(index);
    }

    int size() {
        return entries.size();
    }

    List<LogEntry> copyOfEntries() {
        return List.copyOf(entries);
    }

    RaftLog copy() {
        RaftLog log = new RaftLog();
        for (LogEntry entry : entries) {
            log.push(entry);
        }
        return log;
    }

    void removeEntriesFrom(int indexIncluded) {
        int size = size();
        if (indexIncluded < 0) {
            throw new IndexOutOfBoundsException("index=" + indexIncluded + ", logSize=" + size);
        }
        if (indexIncluded >= size) {
            return;
        }
        for (int i = 0; i < (size - indexIncluded); i++) {
            pop();
        }
    }

    List<LogEntry> getEntriesFrom(int indexIncluded) {
        int size = size();
        if (indexIncluded != 0 && indexIncluded > size) {
            throw new IllegalArgumentException("index=" + indexIncluded + ", logSize=" + size);
        }
        return List.copyOf(entries.subList(indexIncluded, size));
    }


    /**
     * @param o other log
     * @return 1 if this log is more up-to-date; -1 if other log is more up-to-date; 0 if logs are equal
     */
    @Override
    public int compareTo(RaftLog o) {
        if (this.lastLogTerm() == o.lastLogTerm()) {
            if (this.size() > o.size()) {
                return 1;
            }
            if (this.size() == o.size()) {
                return 0;
            }
            if (this.size() < o.size()) {
                return -1;
            }
        } else if (this.lastLogTerm() > o.lastLogTerm()) {
            return 1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftLog)) return false;

        RaftLog raftLog = (RaftLog) o;

        return entries.equals(raftLog.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
