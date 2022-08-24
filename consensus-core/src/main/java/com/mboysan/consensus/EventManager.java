package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class EventManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventManager.class);

    private static final EventManager INSTANCE = new EventManager();

    private final Map<Class<? extends IEvent>, EventConsumers<? extends IEvent>> eventConsumerMap = new ConcurrentHashMap<>();

    private EventManager() {}

    @SuppressWarnings("unchecked")
    private synchronized <T extends IEvent> void registerEventListener0(Class<T> type, Consumer<T> eventConsumer) {
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
    private <T extends IEvent> void fireEvent0(T event) {
        Objects.requireNonNull(event);
        LOGGER.debug("firing event {}", event);
        EventConsumers<T> container = (EventConsumers<T>) eventConsumerMap.get(event.getClass());
        if (container != null) {
            for (Consumer<T> consumer : container.consumers) {
                try {
                    consumer.accept(event);
                } catch (Exception e) {
                    LOGGER.error("event could not be consumed, event={}", event, e);
                }
            }
        }
    }

    private <T extends IEvent> boolean listenerExists0(Class<T> type) {
        return eventConsumerMap.get(type) != null;
    }

    private static class EventConsumers<T extends IEvent> {
        private final List<Consumer<T>> consumers = new ArrayList<>();
    }

    public static <T extends IEvent> void registerEventListener(Class<T> type, Consumer<T> eventConsumer) {
        INSTANCE.registerEventListener0(type, eventConsumer);
    }

    public static <T extends IEvent> void fireEvent(T event) {
        INSTANCE.fireEvent0(event);
    }

    public static <T extends IEvent> boolean listenerExists(Class<T> type) {
        return INSTANCE.listenerExists0(type);
    }
}
