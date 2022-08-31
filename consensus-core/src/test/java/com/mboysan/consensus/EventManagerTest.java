package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class EventManagerTest {

    @Test
    void testEventRegistrationAndFire() {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        // register 2 listeners which listens to any event types.
        EventManager.getInstance().registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManager.getInstance().registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManager.getInstance().fireEvent(mockEvent); // fire an event with specific context.
        assertEquals(2, i.get());
    }

    @Test
    void testAsyncFireEvent() throws InterruptedException {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        CountDownLatch latch = new CountDownLatch(1);
        // register 1 listeners which listens to any event types.
        EventManager.getInstance().registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
            latch.countDown();
        });
        EventManager.getInstance().fireEventAsync(mockEvent); // fire an async event with specific context.
        latch.await();
        assertEquals(1, i.get());
    }

    @Test
    void testConsumerThrowsException() {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        // register 2 listeners which listens to any event types.
        // first listener throws runtime exception, but other should not be affected.
        EventManager.getInstance().registerEventListener(mockEvent.getClass(), e -> {
            throw new RuntimeException();
        });
        EventManager.getInstance().registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManager.getInstance().fireEvent(mockEvent); // fire an event with specific context.
        assertEquals(1, i.get());
    }

    @Test
    void testListenerExists() {
        class SomeEvent implements IEvent {}
        assertFalse(EventManager.getInstance().listenerExists(SomeEvent.class));
    }
}