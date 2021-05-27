package com.mboysan.dist.consensus.raft;

import java.util.ArrayList;
import java.util.List;

public class RaftLog implements Comparable<RaftLog> {
    private final List<LogEntry> entries = new ArrayList<>();

    int lastLogIndex() {
        return entries.size();
    }

    int lastLogTerm() {
        return logTerm(entries.size());
    }

    int logTerm(int index) {
        if (index < 1 || index > entries.size()) {
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
        if (index < 1 || index > entries.size()) {
            throw new IndexOutOfBoundsException("out of bounds index=" + index + ", Note: log index starts from 1");
        }
        return entries.get(index -1);
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
