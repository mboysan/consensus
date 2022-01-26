package com.mboysan.consensus;

import com.mboysan.consensus.message.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @Test
    void testInit() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.size());
        assertEquals(0, log.copyOfEntries().size());
        assertEquals(0, log.lastLogTerm());
        assertEquals(-1, log.lastLogIndex());
    }

    @Test
    void testPush() {
        RaftLog log = new RaftLog();
        log.push(new LogEntry("", 1));
        log.push(new LogEntry("", 2));
        assertEquals(2, log.size());
        assertEquals(1, log.lastLogIndex());

        LogEntry entry = new LogEntry("", 0); // lastLogTerm > 0
        assertThrows(IllegalArgumentException.class, () -> log.push(entry));
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

        assertEquals(entry1.getTerm(), log.logTerm(0));
        assertEquals(entry2.getTerm(), log.logTerm(1));
        assertEquals(entry3.getTerm(), log.logTerm(2));

        assertEquals(0, log.logTerm(-1));
        assertEquals(0, log.logTerm(3));
    }

    @Test
    void testGet() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);

        log.push(entry1);

        assertEquals(entry1, log.get(0));

        assertThrows(IndexOutOfBoundsException.class, () -> log.get(1));
    }

    @Test
    void testRemoveEntriesFrom() {
        RaftLog log = new RaftLog();
        assertThrows(IndexOutOfBoundsException.class, () -> log.removeEntriesFrom(-1));
        log.removeEntriesFrom(0);   // nothing will be removed and no exception will be thrown
        log.removeEntriesFrom(1);   // nothing will be removed and no exception will be thrown

        LogEntry expectedEntry = new LogEntry("", 1);
        log.push(expectedEntry);
        log.push(new LogEntry("", 2));
        log.push(new LogEntry("", 3));

        log.removeEntriesFrom(1);
        assertEquals(1, log.size());

        assertEquals(expectedEntry, log.get(0));
    }

    @Test
    void testEntriesInRange() {
        RaftLog log = new RaftLog();
        assertNotNull(log.getEntriesFrom(0));
        assertEquals(0, log.getEntriesFrom(0).size());

        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);
        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        // lets first test bounds
        assertThrows(IndexOutOfBoundsException.class, () -> log.getEntriesFrom(-1));
        assertThrows(IllegalArgumentException.class, () -> log.getEntriesFrom(4));

        List<LogEntry> actualEntries = log.getEntriesFrom(2);
        assertEquals(1, actualEntries.size());
        assertEquals(actualEntries.get(0), entry3);

        actualEntries = log.getEntriesFrom(1);
        assertEquals(2, actualEntries.size());
        assertEquals(actualEntries.get(0), entry2);
        assertEquals(actualEntries.get(1), entry3);

        actualEntries = log.getEntriesFrom(0);
        assertEquals(3, actualEntries.size());
        assertEquals(actualEntries.get(0), entry1);
        assertEquals(actualEntries.get(1), entry2);
        assertEquals(actualEntries.get(2), entry3);
    }

    @Test
    void testCopyOfEntries() {
        RaftLog log = new RaftLog();
        LogEntry entry1 = new LogEntry("", 1);
        LogEntry entry2 = new LogEntry("", 2);
        LogEntry entry3 = new LogEntry("", 3);
        log.push(entry1);
        log.push(entry2);
        log.push(entry3);

        Collection<LogEntry> copyOfElements = log.copyOfEntries();
        assertEquals(log.size(), copyOfElements.size());
        int i = 0;
        for (LogEntry copyOfElement : copyOfElements) {
            assertEquals(log.get(i++), copyOfElement);
        }

        assertThrows(UnsupportedOperationException.class, () -> copyOfElements.add(entry3));
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

    @Test
    void testCopyRaftLog() {
        RaftLog log = new RaftLog();
        log.push(new LogEntry("cmd1", 1));
        log.push(new LogEntry("cmd2", 2));

        RaftLog logCopy = log.copy();
        assertEquals(log, logCopy);

        logCopy.pop();
        assertNotEquals(log, logCopy);
    }

    @Test
    void testEqualsAndHashcode() {
        RaftLog log1 = new RaftLog();
        log1.push(new LogEntry("cmd1", 1));
        log1.push(new LogEntry("cmd2", 2));

        RaftLog log2 = new RaftLog();
        log2.push(new LogEntry("cmd1", 1));
        log2.push(new LogEntry("cmd2", 2));
        log2.push(new LogEntry("cmd3", 3));

        assertNotEquals(log1, log2);
        assertNotEquals(log1.hashCode(), log2.hashCode());

        log2.pop();

        assertEquals(log1, log2);
        assertEquals(log1.hashCode(), log2.hashCode());
    }
}