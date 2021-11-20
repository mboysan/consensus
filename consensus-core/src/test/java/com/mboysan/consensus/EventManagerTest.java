package com.mboysan.consensus;

import com.mboysan.consensus.event.IEvent;
import com.mboysan.consensus.util.MultiThreadExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventManagerTest {

    @Test
    void testSingleInstance() throws ExecutionException, InterruptedException {
        MultiThreadExecutor executor = new MultiThreadExecutor();
        EventManager[] emArr = new EventManager[10];
        for (int i = 0; i < emArr.length; i++) {
            int finalI = i;
            executor.execute(() -> emArr[finalI] = EventManager.getInstance());
        }
        executor.endExecution();

        EventManager expected = EventManager.getInstance();
        for (EventManager actual : emArr) {
            assertEquals(expected, actual);
        }
    }

    @Test
    void testEventRegistrationAndFire() {
        EventManager manager = EventManager.getInstance();
        AtomicInteger i = new AtomicInteger(0);
        IEvent mockEvent = Mockito.mock(IEvent.class);
        // register 2 listeners which listens to any event types.
        manager.registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        manager.registerEventListener(mockEvent.getClass(), e -> {
            i.incrementAndGet();    // perform some operation
        });
        manager.fireEvent(mockEvent); // fire an event with specific context.
        assertEquals(2, i.get());
    }

}