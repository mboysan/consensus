package com.mboysan.consensus.util;

/**
 * A {@link Runnable} that throws throwable.
 */
@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Throwable;
}
