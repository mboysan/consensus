package com.mboysan.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class BackgroundServiceRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackgroundServiceRegistry.class);

    private static final BackgroundServiceRegistry INSTANCE = new BackgroundServiceRegistry();

    private final List<BackgroundService> services = new ArrayList<>();

    public synchronized void register(BackgroundService service) {
        services.add(service);
        LOGGER.info("registered new background service={}", service);
    }

    public synchronized void shutdownAll() {
        LOGGER.info("shutting down background services={}", services);
        Iterator<BackgroundService> iter = services.iterator();
        while(iter.hasNext()) {
            BackgroundService service = iter.next();
            try {
                service.shutdown();
            } catch (Exception e) {
                LOGGER.error("error when shutting down service={}", service, e);
            } finally {
                iter.remove();
            }
        }
    }

    public static BackgroundServiceRegistry getInstance() {
        return INSTANCE;
    }
}
