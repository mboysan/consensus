package com.mboysan.consensus.util;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public final class AwaitUtil {

    private static final long DEFAULT_AWAIT_SECONDS = 20;

    private AwaitUtil() {

    }

    public static <T> T awaiting(CheckedSupplier<T, Throwable> supplier) {
        return awaiting(null, supplier);
    }

    public static <T> T awaiting(
            Class<? extends Exception> expectedExceptionType,
            CheckedSupplier<T, Throwable> supplier)
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

    public static void awaiting(CheckedRunnable<Throwable> runnable) {
        awaiting(null, runnable);
    }

    public static void awaiting(
            Class<? extends Exception> expectedExceptionType,
            CheckedRunnable<Throwable> runnable)
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
}
