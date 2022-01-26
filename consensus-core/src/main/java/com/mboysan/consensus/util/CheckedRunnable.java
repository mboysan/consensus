package com.mboysan.consensus.util;

/**
 * A {@link Runnable} that throws a checked exception.
 *
 * @param <E> the type of the {@link Throwable} {@link #run()} method might throw.
 */
@FunctionalInterface
public interface CheckedRunnable<E extends Throwable> {
    void run() throws E;
}
