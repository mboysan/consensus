package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class EventManagerServiceTest {

    @Test
    void testEventRegistrationAndFire() {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        // register 2 listeners which listens to any event types.
        EventManagerService.getInstance().register(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManagerService.getInstance().register(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManagerService.getInstance().fire(mockEvent); // fire an event with specific context.
        assertEquals(2, i.get());
    }

    @Test
    void testAsyncFireEvent() throws InterruptedException {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        CountDownLatch latch = new CountDownLatch(1);
        // register 1 listeners which listens to any event types.
        EventManagerService.getInstance().register(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
            latch.countDown();
        });
        EventManagerService.getInstance().fireAsync(mockEvent); // fire an async event with specific context.
        latch.await();
        assertEquals(1, i.get());
    }

    @Test
    void testConsumerThrowsException() {
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = mock(IEvent.class);
        // register 2 listeners which listens to any event types.
        // first listener throws runtime exception, but other should not be affected.
        EventManagerService.getInstance().register(mockEvent.getClass(), e -> {
            throw new RuntimeException();
        });
        EventManagerService.getInstance().register(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        EventManagerService.getInstance().fire(mockEvent); // fire an event with specific context.
        assertEquals(1, i.get());
    }

    @Test
    void testDeregisterEventConsumer() {
        class SomeEvent implements IEvent {}

        AtomicInteger i = new AtomicInteger(0);
        Consumer<SomeEvent> consumer1 = e -> {
            i.incrementAndGet();    // perform some operation
        };
        Consumer<SomeEvent> consumer2 = e -> {
            i.incrementAndGet();    // perform some operation
        };
        // register 2 listeners which listens to any event types.
        EventManagerService.getInstance().register(SomeEvent.class, consumer1);
        EventManagerService.getInstance().register(SomeEvent.class, consumer2);
        EventManagerService.getInstance().fire(new SomeEvent()); // fire an event with specific context.
        assertEquals(2, i.get());

        // now, deregister one of the consumers
        boolean result = EventManagerService.getInstance().deregister(consumer1);
        assertTrue(result);
        EventManagerService.getInstance().fire(new SomeEvent()); // fire an event with specific context.
        assertEquals(3, i.get());
    }

    @Test
    void testListenerExists() {
        class SomeEvent implements IEvent {}
        assertFalse(EventManagerService.getInstance().listenerExists(SomeEvent.class));
    }
}