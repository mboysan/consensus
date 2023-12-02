package com.mboysan.consensus.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mboysan.consensus.util.AwaitUtil.awaiting;
import static org.junit.jupiter.api.Assertions.*;

public class AwaitUtilTest {

    @Test
    void testAwaitingSupplierWithExpectedException() {

        Class<TimeoutException> expectedExceptionType = TimeoutException.class;

        AtomicBoolean failedOnce = new AtomicBoolean(false);
        awaiting(expectedExceptionType, () -> {
            if (failedOnce.get()) {
                return null;
            }
            failedOnce.set(true);

            // expected exception is still fine even if it's nested
            throw new RuntimeException(new IOException(new TimeoutException()));
        });
        assertTrue(failedOnce.get());
    }

    @Test
    void testAwaitingRunnableWithExpectedException() {

        Class<TimeoutException> expectedExceptionType = TimeoutException.class;

        AtomicBoolean failedOnce = new AtomicBoolean(false);
        awaiting(expectedExceptionType, () -> {
            if (failedOnce.get()) {
                return;
            }
            failedOnce.set(true);

            // expected exception is still fine even if it's nested
            throw new RuntimeException(new IOException(new TimeoutException()));
        });
        assertTrue(failedOnce.get());
    }

    @Test
    void testAwaitingSupplierWithUnexpectedExpectedException() {
        Class<TimeoutException> expectedExceptionType = TimeoutException.class;
        assertThrows(RuntimeException.class, () -> {
            ThrowingSupplier<?> supplier = () -> {
                throw new RuntimeException(new IOException());
            };
            awaiting(expectedExceptionType, supplier);
        });
    }

    @Test
    void testAwaitingRunnableWithUnexpectedExpectedException() {
        Class<TimeoutException> expectedExceptionType = TimeoutException.class;
        assertThrows(RuntimeException.class, () -> {
            ThrowingRunnable runnable = () -> {
                throw new RuntimeException(new IOException());
            };
            awaiting(expectedExceptionType, runnable);
        });
    }

    @Test
    void testAwaitingSupplierSuccess() {
        assertDoesNotThrow(() -> {
            ThrowingSupplier<?> supplier = () -> null;
            awaiting(supplier);
        });
    }

    @Test
    void testAwaitingRunnableSuccess() {
        assertDoesNotThrow(() -> {
            ThrowingRunnable runnable = () -> {};
            awaiting(runnable);
        });
    }

}
