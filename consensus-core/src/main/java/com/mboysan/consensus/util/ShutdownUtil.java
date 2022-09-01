package com.mboysan.consensus.util;

import org.apache.commons.pool2.ObjectPool;
import org.slf4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ShutdownUtil {
    private ShutdownUtil() {}

    public static void close(final Logger logger, final ServerSocket serverSocket) {
        shutdown(logger, () -> {
            if (serverSocket != null) {
                serverSocket.close();
            }
        });
    }

    public static void close(final Logger logger, final Socket socket) {
        shutdown(logger, () -> {
            if (socket != null) {
                ShutdownUtil.close(logger, socket.getOutputStream());
                ShutdownUtil.close(logger, socket.getInputStream());
                socket.close();
            }
        });
    }

    public static void close(final Logger logger, final OutputStream outputStream) {
        shutdown(logger, () -> {
            if (outputStream != null) {
                synchronized (outputStream) {
                    outputStream.close();
                }
            }
        });
    }

    public static void close(final Logger logger, final InputStream inputStream) {
        shutdown(logger, () -> {
            if (inputStream != null) {
                synchronized (inputStream) {
                    inputStream.close();
                }
            }
        });
    }

    public static void close(final Logger logger, final ObjectPool<?> pool) {
        shutdown(logger, () -> {
            if (pool != null) {
                pool.close();
            }
        });
    }

    public static void shutdown(final Logger logger, final ExecutorService executor) {
        if (executor != null) {
            shutdown(logger, executor::shutdown);
            shutdown(logger, () -> {
                boolean success = executor.awaitTermination(5000L, TimeUnit.MILLISECONDS);
                if (!success) {
                    logger.warn("termination failed for executor");
                }
            });
        }
    }

    public static void shutdown(final Logger logger, final ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }
}
