package com.mboysan.consensus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private final Map<String, TimerThread> taskMap = new HashMap<>();

    public synchronized void schedule(String taskName, Runnable task, long delay, long period) {
        Objects.requireNonNull(taskName);
        Objects.requireNonNull(task);
        if (delay <= 0 || period <= 0) {
            throw new IllegalArgumentException("delay or period must be greater than zero");
        }
        TimerThread prevTask = taskMap.get(taskName);
        if (prevTask != null) {
            prevTask.cancel();
        }
        TimerThread newTask = new TimerThread(task, taskName, delay, period);
        taskMap.put(taskName, newTask);
        newTask.start();
        LOGGER.info("Timer for task={} scheduled with delay={} and period={}", taskName, delay, period);
    }

    public long currentTime() {
        return System.currentTimeMillis();
    }

    public synchronized void shutdown() {
        taskMap.forEach((s, t) -> t.cancel());
        taskMap.clear();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private static final class TimerThread extends Thread {
        private volatile boolean isRunning = true;
        private final Runnable runnable;
        private final long delay;
        private final long period;

        private TimerThread(Runnable runnable, String name, long delay, long period) {
            this.runnable = runnable;
            this.delay = delay;
            this.period = period;
            setDaemon(false);
            setName(name);
        }

        @Override
        public void run() {
            doSleep(delay);
            while (isRunning) {
                try {
                    runnable.run();
                } catch (Exception e) {
                    if (isInterrupted()) {
                        return;
                    }
                    LOGGER.error(e.getMessage(), e);
                }
                doSleep(period);
            }
        }

        public void cancel() {
            isRunning = false;
            interrupt();
        }

        private void doSleep(long timeout) {
            try {
                sleep(timeout);
            } catch (InterruptedException e) {
                isRunning = false;
                Thread.currentThread().interrupt();
            }
        }
    }
}
