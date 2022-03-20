package com.mboysan.consensus.util;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public final class AwaitUtil {

    private static final long DEFAULT_AWAIT_SECONDS = 20;

    private AwaitUtil() {

    }

    public static <T> T awaiting(ThrowingSupplier<T> supplier) {
        return awaiting(null, supplier);
    }

    public static <T> T awaiting(
            Class<? extends Exception> expectedExceptionType,
            ThrowingSupplier<T> supplier)
    {
        AtomicReference<T> ref = new AtomicReference<>();
        await().atMost(DEFAULT_AWAIT_SECONDS, SECONDS).untilAsserted(() -> {
            try {
                ref.set(supplier.get());
            } catch (Throwable t) {
                if (expectedExceptionType == null || t.getClass().isAssignableFrom(expectedExceptionType)) {
                    throw new AssertionError(t);
                }
                throw t;
            }
        });
        return ref.get();
    }

    public static void awaiting(ThrowingRunnable runnable) {
        awaiting(null, runnable);
    }

    public static void awaiting(
            Class<? extends Exception> expectedExceptionType,
            ThrowingRunnable runnable)
    {
        await().atMost(DEFAULT_AWAIT_SECONDS, SECONDS).untilAsserted(() -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                if (expectedExceptionType == null || t.getClass().isAssignableFrom(expectedExceptionType)) {
                    throw new AssertionError(t);
                }
                throw t;
            }
        });
    }

    public static void awaitingAtLeast(long milliseconds, ThrowingRunnable runnable) throws Exception {
        Thread.sleep(milliseconds);
        try {
            runnable.run();
        } catch (Throwable t) {
            throw new Error(t);
        }
    }
}
