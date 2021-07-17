package com.mboysan.consensus;

import java.util.Set;

public interface KVStore {
    void start() throws Exception;
    void shutdown() throws Exception;

    boolean put(String key, String value);
    String get(String key);
    boolean remove(String key);
    Set<String> keySet();
}
