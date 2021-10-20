package com.mboysan.consensus.util;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimersForTesting implements Timers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimersForTesting.class);

    private static final long SEED = 1L;

    private Random random = new Random(SEED);
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
        random = new Random(SEED);
    }

    public void runAll() {
        if (sortedTasks.size() == 0) {
            return;
        }
        long lastToRunTime = sortedTasks.last().timeToRun;
        while (currentTime <= lastToRunTime) {
            runNext();
            if (sortedTasks.first().timeToRun > lastToRunTime) {
                break;
            }
        }
    }

    /**
     * Advances time to {@link #currentTime} + <tt>time</tt>
     *
     * @param time time to add to {@link #currentTime}.
     */
    public void advance(long time) {
        if (sortedTasks.size() == 0) {
            return;
        }
        long advanceTo = currentTime + time;
        while (currentTime <= advanceTo) {
            Task task = sortedTasks.pollFirst();
            assertNotNull(task);
            if (task.timeToRun <= advanceTo) {
                currentTime = task.timeToRun;
                if (!task.isPaused) {
                    task.run();
                }
                task.timeToRun = currentTime + task.period;
                sortedTasks.add(task);
            } else {
                // first task is ahead of given time
                currentTime = advanceTo;
                sortedTasks.add(task);  // put it back
                break;
            }
        }
    }

    private void runNext() {
        Task task = sortedTasks.pollFirst();
        assertNotNull(task);
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

    private class Task implements Comparable<Task> {
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
            if (timeToRun == o.timeToRun) {
                // randomize insert order to simulate thread runs.
                return random.nextBoolean() ? 1 : -1;
            }
            return timeToRun > o.timeToRun ? 1 : -1;
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
