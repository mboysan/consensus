package com.mboysan.util;

public interface Timers {
    void schedule(String taskName, Runnable task, long delay, long period);
    void shutdown();
}
