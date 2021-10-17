package com.mboysan.consensus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class TimerQueue implements Timers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimerQueue.class);

    private volatile boolean isRunning;
    private final Timer queue = new Timer(true);
    private final Map<String, TimerTask> taskMap = new HashMap<>();

    public TimerQueue() {
        isRunning = true;
    }

    @Override
    public synchronized void schedule(String taskName, Runnable task, long delay, long period) {
        Objects.requireNonNull(taskName);
        Objects.requireNonNull(task);
        if (delay <= 0 || period <= 0) {
            throw new IllegalArgumentException("delay or period must be greater than zero");
        }
        if (!isRunning) {
            return;
        }
        TimerTask prevTask = taskMap.get(taskName);
        if (prevTask != null) {
            prevTask.cancel();
        }
        TimerTask newTask = new TimerTask() {
            @Override
            public void run() {
                task.run();
            }
        };
        taskMap.put(taskName, newTask);
        queue.schedule(newTask, delay, period);
        LOGGER.info("Timer for task={} scheduled with delay={} and period={}", taskName, delay, period);
    }

    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }

    @Override
    public synchronized void shutdown() {
        isRunning = false;
        taskMap.clear();
        queue.cancel();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
