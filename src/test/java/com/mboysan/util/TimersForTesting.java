package com.mboysan.util;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class TimersForTesting implements Timers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimersForTesting.class);

    private long currentTime = 0;

    private final Map<String, Task> taskMap = new ConcurrentHashMap<>();
    private final TreeSet<Task> sortedTasks = new TreeSet<>();

    @Override
    public void schedule(String taskName, Runnable runnable, long delay, long period) {
        Assertions.assertNull(taskMap.get(taskName));
        Task task = new Task();
        task.name = taskName;
        task.runnable = runnable;
        task.period = period;
        task.timeToRun = delay;
        taskMap.put(taskName, task);
        sortedTasks.add(task);
        LOGGER.info("Timer for task={} scheduled with delay={} and period={}", taskName, delay, period);
    }

    @Override
    public long currentTime() {
        return currentTime;
    }

    @Override
    public void shutdown() {
        currentTime = 0;
        taskMap.clear();
        sortedTasks.clear();
    }

    public void runAll() {
        if (sortedTasks.size() == 0) {
            return;
        }
        long lastToRunTime = sortedTasks.last().timeToRun;
        while(currentTime <= lastToRunTime) {
            runNext();
            if (sortedTasks.first().timeToRun > lastToRunTime) {
                break;
            }
        }
    }

    private void runNext() {
        Task task = sortedTasks.pollFirst();
        currentTime = task.timeToRun;
        if (!task.isPaused) {
            task.run();
        }
        task.timeToRun = currentTime + task.period;
        sortedTasks.add(task);
    }


    public synchronized void pause(String taskName) {
        taskMap.get(taskName).isPaused = true;
    }

    public synchronized void resume(String taskName) {
        taskMap.get(taskName).isPaused = false;
    }

    @Override
    public void sleep(long ms) {
        // no action
    }

    private static class Task implements Comparable<Task> {
        String name;
        long period;
        Runnable runnable;
        long timeToRun;
        boolean isPaused = false;

        void run() {
            try {
                LOGGER.debug("running task: {}", this);
                runnable.run();
            } catch (Throwable t) {
                LOGGER.error("ignored: {}", t.getMessage());
            }
        }

        @Override
        public int compareTo(Task o) {
            return timeToRun >= o.timeToRun ? 1 : -1;
        }

        @Override
        public String toString() {
            return "Task{" +
                    "name='" + name + '\'' +
                    ", period=" + period +
                    ", timeToRun=" + timeToRun +
                    '}';
        }
    }
}
