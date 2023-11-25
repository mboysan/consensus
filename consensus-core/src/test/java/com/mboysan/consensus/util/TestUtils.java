package com.mboysan.consensus.util;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {
    }

    public static void logTestName(TestInfo testInfo) {
        String testClassName = testInfo.getTestClass().orElse(TestUtils.class).getSimpleName();
        String testMethodName = testInfo.getDisplayName();
        LOGGER.info("Running Test: [{}.{}]", testClassName, testMethodName);
    }
}
