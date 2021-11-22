package com.mboysan.consensus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * A utility class that is used as an executor but used to report any exceptions caught when
 * {@link #execute(CheckedRunnable)} method is run.
 */
public class MultiThreadExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadExecutor.class);

    /**
     * Executes the registered runnables.
     */
    private final ExecutorService executor;
    /**
     * Total executions required.
     */
    private final List<Future<Optional<Exception>>> futures = new ArrayList<>();

    private final String execId = UUID.randomUUID().toString();

    // enables overlapping of threads.
    private final CountDownLatch latch = new CountDownLatch(1);

    public MultiThreadExecutor() {
        this(Runtime.getRuntime().availableProcessors() * 2);
    }

    public MultiThreadExecutor(int threadCount) {
        executor = Executors.newFixedThreadPool(threadCount);
    }

    /**
     * Runs the <tt>runnable</tt> passed with the {@link #executor}. Catches any exceptions caught during the execution
     * of the <tt>runnable</tt>.
     * @param runnable the runnable to handle and report any ee.ut.jbizur.exceptions caught when running it.
     */
    public void execute(CheckedRunnable runnable) {
        futures.add(executor.submit(() -> {
            try {
                latch.await();
                runnable.run();
                return Optional.empty();
            } catch (Exception e) {
                return Optional.of(e);
            }
        }));
    }

    /**
     * Waits for all the executions to complete.
     * @throws InterruptedException if latch await fails.
     */
    public void endExecution() throws InterruptedException, ExecutionException {
        LOGGER.info("ending execution id=" + execId);
        latch.countDown();
        for (Future<Optional<Exception>> future : futures) {
            Optional<Exception> optEx = future.get();
            optEx.ifPresent(e -> fail(String.format("execution[id=%s] failed with exception=%s", execId, e)));
        }
        executor.shutdown();
        LOGGER.info("execution[id={}] ended successfully.", execId);
    }
}
