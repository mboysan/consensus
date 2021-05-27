package com.mboysan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class TimerQueue implements Timers {
    private static final Logger LOG = LoggerFactory.getLogger(TimerQueue.class);

    private volatile boolean isRunning;
    private final Timer queue = new Timer(true);
    private final Map<String, TimerTask> taskMap = new HashMap<>();

    public TimerQueue() {
        isRunning = true;
    }

    @Override
    public synchronized void schedule(String taskName, Runnable task, long delay, long period) {
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
        LOG.info("Timer for task={} scheduled with delay={} and period={}", taskName, delay, period);
    }

    @Override
    public synchronized void shutdown() {
        isRunning = false;
        taskMap.clear();
        queue.cancel();
    }
}
