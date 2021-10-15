package com.mboysan.consensus.util;

public interface Timers {
    void schedule(String taskName, Runnable task, long delay, long period);

    long currentTime();

    void shutdown();

    void sleep(long ms);
}
