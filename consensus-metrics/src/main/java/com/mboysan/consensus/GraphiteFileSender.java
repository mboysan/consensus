package com.mboysan.consensus;

import com.codahale.metrics.graphite.GraphiteSender;
import com.mboysan.consensus.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class GraphiteFileSender implements GraphiteSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteFileSender.class);

    private volatile boolean isRunning;
    private final String separator;
    private final PrintWriter writer;

    public GraphiteFileSender(String outputPath, String separator) {
        try {
            this.separator = separator;
            Path path = FileUtil.path(outputPath);
            Files.deleteIfExists(path);
            Files.createFile(path);
            File file = path.toFile();
            boolean writable = file.setWritable(true);
            boolean readable = file.setReadable(true);
            if (!(writable && readable)) {
                throw new IllegalStateException("read/write not supported on path");
            }
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
            LOGGER.info("metrics file: {}", path.toAbsolutePath());
            isRunning = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void connect() {
        // not needed
    }

    @Override
    public void send(String name, String value, long timestamp) {
        writer.println(name + separator + value + separator + timestamp);
    }

    @Override
    public void flush() {
        writer.flush();
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public int getFailures() {
        return 0;
    }

    @Override
    public void close() {
        // not needed
    }

    public void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        writer.close();
    }
}
