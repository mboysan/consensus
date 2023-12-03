package com.mboysan.consensus.util;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class AwaitUtil {

    private static final long DEFAULT_AWAIT_SECONDS = 20;
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 500;

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
        await().untilAsserted(() -> {
            try {
                ref.set(supplier.get());
            } catch (Throwable t) {
                if (isErrorExpected(t, expectedExceptionType)) {
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
        await().untilAsserted(() -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                if (isErrorExpected(t, expectedExceptionType)) {
                    throw new AssertionError(t);
                }
                throw t;
            }
        });
    }

    public static void doSleep(long milliseconds) {
        awaitingAtLeast(milliseconds, () -> {});
    }

    public static void awaitingAtLeast(long milliseconds, ThrowingRunnable runnable) {
        try {
            Thread.sleep(milliseconds);
            runnable.run();
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private static ConditionFactory await() {
        return Awaitility.await()
                .atMost(DEFAULT_AWAIT_SECONDS, SECONDS)
                .pollInterval(DEFAULT_POLL_INTERVAL_MILLIS, MILLISECONDS)
                .pollDelay(0, MILLISECONDS);
    }

    private static boolean isErrorExpected(Throwable throwable, Class<? extends Exception> expectedExceptionType) {
        return expectedExceptionType == null
                || throwable.getClass().isAssignableFrom(expectedExceptionType)
                || (throwable.getCause() != null && isErrorExpected(throwable.getCause(), expectedExceptionType));
    }
}
