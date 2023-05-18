package com.mboysan.consensus.util;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class ShutdownUtil {
    private ShutdownUtil() {}

    public static void close(final Logger logger, final Closeable closeable) {
        shutdown(logger, () -> {
            if (closeable != null) {
                closeable.close();
            }
        });
    }

    public static void shutdown(final Logger logger, final ExecutorService executor) {
        if (executor != null) {
            shutdown(logger, executor::shutdown);
            shutdown(logger, () -> {
                boolean success = executor.awaitTermination(5000L, TimeUnit.MILLISECONDS);
                if (!success) {
                    final String nl = System.getProperty("line.separator");
                    String stackTrace = Arrays.stream(Thread.currentThread().getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining(nl + "\t at "));
                    logger.warn("termination failed for executor. stackTrace -> {}", stackTrace);
                }
            });
        }
    }

    public static void shutdown(final Logger logger, final ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }
}
