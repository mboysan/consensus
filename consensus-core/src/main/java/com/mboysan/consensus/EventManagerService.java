package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import com.mboysan.consensus.util.ShutdownUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public final class EventManagerService implements BackgroundService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventManagerService.class);

    private static final EventManagerService INSTANCE = new EventManagerService();

    private final Map<Class<? extends IEvent>, EventConsumers<? extends IEvent>> eventConsumerMap = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new BasicThreadFactory.Builder().namingPattern("event-manager-%d").daemon(true).build()
    );

    private volatile boolean isRunning = true;

    private EventManagerService() {
        BackgroundServiceRegistry.getInstance().register(this);
    }

    @SuppressWarnings("unchecked")
    public synchronized <T extends IEvent> void register(Class<T> eventType, Consumer<T> eventConsumer) {
        if (!isRunning) {
            return;
        }
        EventConsumers<T> eventConsumers = (EventConsumers<T>) eventConsumerMap.get(eventType);
        if (eventConsumers == null) {
            eventConsumers = new EventConsumers<>();
            eventConsumers.consumers.add(eventConsumer);
            eventConsumerMap.put(eventType, eventConsumers);
        } else {
            eventConsumers.consumers.add(eventConsumer);
        }

    }

    @SuppressWarnings("unchecked")
    public <T extends IEvent> void fire(T event) {
        if (!isRunning) {
            return;
        }
        Objects.requireNonNull(event);
        LOGGER.debug("firing event {}", event);
        EventConsumers<T> container = (EventConsumers<T>) eventConsumerMap.get(event.getClass());
        if (container != null) {
            for (Consumer<T> consumer : container.consumers) {
                try {
                    consumer.accept(event);
                } catch (Exception e) {
                    LOGGER.error("error occurred while consuming the event={}", event, e);
                }
            }
        }
    }

    public <T extends IEvent> void fireAsync(T event) {
        if (!isRunning) {
            return;
        }
        executor.submit(() -> fire(event));
    }

    public <T extends IEvent> boolean listenerExists(Class<T> eventType) {
        return eventConsumerMap.get(eventType) != null;
    }

    @Override
    public void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        ShutdownUtil.shutdown(LOGGER, executor);
    }

    @Override
    public String toString() {
        return "EventManagerService";
    }

    private static class EventConsumers<T extends IEvent> {
        private final List<Consumer<T>> consumers = new ArrayList<>();
    }

    public static EventManagerService getInstance() {
        return INSTANCE;
    }
}
