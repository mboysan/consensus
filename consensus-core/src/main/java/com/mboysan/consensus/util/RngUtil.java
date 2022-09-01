package com.mboysan.consensus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public final class RngUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RngUtil.class);
    private static final SecureRandom RNG;
    static {
        String seed = System.currentTimeMillis() + "";
        LOGGER.info("message RNG seed = {}", seed);
        RNG = new SecureRandom(seed.getBytes(StandardCharsets.UTF_8));
    }

    private RngUtil() {}

    public static int nextInt(int bound) {
        return RNG.nextInt(bound);
    }

    public static long nextLong(long origin, long bound) {
        return RNG.nextLong(origin, bound);
    }

    public static boolean nextBoolean() {
        return RNG.nextBoolean();
    }
}
