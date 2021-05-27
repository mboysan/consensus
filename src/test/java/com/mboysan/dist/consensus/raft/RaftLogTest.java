package com.mboysan.dist.consensus.raft;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftLogTest {

    @Test
    void testInit() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.size());
        assertEquals(0, log.copyOfEntries().size());
        assertEquals(0, log.lastLogTerm());
        assertEquals(0, log.lastLogIndex());
    }

    @Test
    void testPush() {
        RaftLog log = new RaftLog();
        log.push(new LogEntry("", 1));
        log.push(new LogEntry("", 2));
        assertEquals(2, log.size());
        assertEquals(2, log.lastLogIndex());

        assertThrows(IllegalArgumentException.class, () ->
                log.push(new LogEntry("", 0))   // lastLogTerm > 0
        );
    }

    @Test
    void testPushPop() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);

        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        assertEquals(entry3, log.pop());
        assertEquals(entry2, log.pop());
        assertEquals(entry1, log.pop());
        assertEquals(0, log.size());

        assertThrows(IndexOutOfBoundsException.class, log::pop);
    }

    @Test
    void testLastLogTerm() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);

        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        assertEquals(entry3.getTerm(), log.lastLogTerm());
    }

    @Test
    void testLogTerm() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);

        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        assertEquals(entry1.getTerm(), log.logTerm(1));
        assertEquals(entry2.getTerm(), log.logTerm(2));
        assertEquals(entry3.getTerm(), log.logTerm(3));

        assertEquals(0, log.logTerm(0));    //???
        assertEquals(0, log.logTerm(4));
    }

    @Test
    void testGet() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);

        log.push(entry1);

        assertEquals(entry1, log.get(1));

        assertThrows(IndexOutOfBoundsException.class, () -> log.get(0));
        assertThrows(IndexOutOfBoundsException.class, () -> log.get(2));
    }

    @Test
    void testCopy() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);
        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        Collection<LogEntry> copyOfElements = log.copyOfEntries();
        assertEquals(log.size(), copyOfElements.size());
        int i = 1;
        for (LogEntry copyOfElement : copyOfElements) {
            assertEquals(log.get(i++), copyOfElement);
        }

        assertThrows(UnsupportedOperationException.class, () -> {
            copyOfElements.add(new LogEntry("", 3));
        });
    }

    @Test
    void testCompareToWhenEquals() {
        RaftLog log1 = new RaftLog();
        RaftLog log2 = new RaftLog();
        assertEquals(0, log1.compareTo(log2));

        log1.push(new LogEntry("", 1));
        log1.push(new LogEntry("", 2));
        log2.push(new LogEntry("", 2)); // TODO: is this an issue?
        log2.push(new LogEntry("", 2));
        assertEquals(0, log1.compareTo(log2));
    }

    @Test
    void testCompareToWhenNotUpToDate() {
        RaftLog log1 = new RaftLog();
        RaftLog log2 = new RaftLog();

        log1.push(new LogEntry("", 1));
        assertEquals(1, log1.compareTo(log2));  // log1 is more up-to-date

        log2.push(new LogEntry("", 2));
        assertEquals(-1, log1.compareTo(log2));  // log1 is less up-to-date

        log1.push(new LogEntry("", 2)); // log1 is longer now
        assertEquals(1, log1.compareTo(log2));  // hence, more up-to-date

        log2.push(new LogEntry("", 2)); // log sizes are equal now
        assertEquals(0, log1.compareTo(log2));  // both are up-to-date

        log2.push(new LogEntry("", 2)); // log2 is longer now
        assertEquals(-1, log1.compareTo(log2));  // log1 is less up-to-date
    }
}