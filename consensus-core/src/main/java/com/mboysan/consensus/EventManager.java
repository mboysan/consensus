package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import com.mboysan.consensus.util.ThrowingRunnable;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class EventManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventManager.class);

    private static final EventManager INSTANCE = new EventManager();

    private final Map<Class<? extends IEvent>, EventConsumers<? extends IEvent>> eventConsumerMap = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            new BasicThreadFactory.Builder().namingPattern("event-manager-%d").daemon(true).build()
    );

    private volatile boolean isRunning = true;

    private EventManager() {}

    @SuppressWarnings("unchecked")
    public synchronized <T extends IEvent> void registerEventListener(Class<T> type, Consumer<T> eventConsumer) {
        if (!isRunning) {
            return;
        }
        EventConsumers<T> eventConsumers = (EventConsumers<T>) eventConsumerMap.get(type);
        if (eventConsumers == null) {
            eventConsumers = new EventConsumers<>();
            eventConsumers.consumers.add(eventConsumer);
            eventConsumerMap.put(type, eventConsumers);
        } else {
            eventConsumers.consumers.add(eventConsumer);
        }

    }

    @SuppressWarnings("unchecked")
    public <T extends IEvent> void fireEvent(T event) {
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

    public <T extends IEvent> void fireEventAsync(T event) {
        if (!isRunning) {
            return;
        }
        executor.submit(() -> fireEvent(event));
    }

    public void shutdown() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        shutdown(executor::shutdown);
        shutdown(() -> executor.awaitTermination(5000L, TimeUnit.MILLISECONDS));
    }

    public <T extends IEvent> boolean listenerExists(Class<T> type) {
        return eventConsumerMap.get(type) != null;
    }

    private static class EventConsumers<T extends IEvent> {
        private final List<Consumer<T>> consumers = new ArrayList<>();
    }

    public static EventManager getInstance() {
        return INSTANCE;
    }

    private static void shutdown(ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
