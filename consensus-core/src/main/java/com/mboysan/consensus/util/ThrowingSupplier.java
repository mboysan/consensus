package com.mboysan.consensus.util;

/**
 * A {@link java.util.function.Supplier Supplier} that throws a throwable.
 *
 * @param <T> the type of results supplied by this supplier.
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Throwable;
}
