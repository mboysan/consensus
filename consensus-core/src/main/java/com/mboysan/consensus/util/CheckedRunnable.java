package com.mboysan.consensus.util;

@FunctionalInterface
public interface CheckedRunnable {
    void run() throws Exception;
}
