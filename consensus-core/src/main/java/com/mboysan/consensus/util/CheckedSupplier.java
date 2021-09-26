package com.mboysan.consensus.util;

/**
 * A {@link java.util.function.Supplier Supplier} that throws a checked exception.
 * @param <T> the type of results supplied by this supplier
 */
@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Exception;
}
