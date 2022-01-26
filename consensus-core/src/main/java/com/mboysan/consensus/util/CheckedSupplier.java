package com.mboysan.consensus.util;

/**
 * A {@link java.util.function.Supplier Supplier} that throws a checked exception.
 *
 * @param <T> the type of results supplied by this supplier
 * @param <E> the type of the {@link Throwable} {@link #get()} method might throw.
 */
@FunctionalInterface
public interface CheckedSupplier<T, E extends Throwable> {
    T get() throws E;
}
