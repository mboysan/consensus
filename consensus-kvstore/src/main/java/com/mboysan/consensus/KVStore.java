package com.mboysan.consensus;

import java.util.Set;

public interface KVStore {
    void start() throws Exception;

    void shutdown() throws Exception;

    boolean put(String key, String value) throws KVOperationException;

    String get(String key) throws KVOperationException;

    boolean remove(String key) throws KVOperationException;

    Set<String> keySet() throws KVOperationException;
}
