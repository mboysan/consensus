package com.mboysan.consensus.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class StateUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateUtil.class);

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    private StateUtil() {}

    public static void writeStateStarted() {
        writeState("consensus_state_started");
    }

    public static void writeState(String state) {
        try {
            Path path = Files.createFile(Path.of(TEMP_DIR, state));
            File file = path.toFile();
            file.deleteOnExit();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
