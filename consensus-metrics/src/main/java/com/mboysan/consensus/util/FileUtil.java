package com.mboysan.consensus.util;

import java.nio.file.Path;

public final class FileUtil {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    private FileUtil() {}

    public static Path path(String pathToFile) {
        if (pathToFile.startsWith("/tmp/")) {
            pathToFile = pathToFile.replaceAll("/tmp/", "");
            return Path.of(TEMP_DIR, pathToFile);
        } else {
            return Path.of(pathToFile);
        }
    }
}
