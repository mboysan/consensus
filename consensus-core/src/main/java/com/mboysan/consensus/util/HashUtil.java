package com.mboysan.consensus.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class HashUtil {

    private HashUtil() {
    }

    public static Optional<String> findCommonHash(Collection<String> hashes, int threshold) {
        return findCommonHash(List.copyOf(hashes), threshold);
    }

    public static Optional<String> findCommonHash(List<String> hashList, int threshold) {
        if (threshold < 0 || threshold > hashList.size() || hashList.isEmpty()) {
            throw new IllegalArgumentException("invalid threshold or hashList");
        }

        Map<String, Integer> occorrencesMap = new HashMap<>();
        for (String hash : hashList) {
            Integer count = occorrencesMap.putIfAbsent(hash, 1);
            occorrencesMap.put(hash, Optional.ofNullable(count).orElse(0) + 1);
        }
        int maxCount = 0;
        String commonHash = null;
        for (String hash : occorrencesMap.keySet()) {
            Integer count = occorrencesMap.get(hash);
            if (count > maxCount) {
                maxCount = count;
                commonHash = hash;
            }
        }
        if (maxCount < threshold) {
            return Optional.empty();
        }
        return Optional.of(commonHash);
    }
}
