package com.mboysan.dist;

public interface KVStore {
    void start() throws Exception;
    void shutdown() throws Exception;

    boolean put(String key, String value);
    String get(String key);
    boolean remove(String key);
}
