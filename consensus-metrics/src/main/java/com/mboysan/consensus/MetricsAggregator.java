package com.mboysan.consensus;

import com.mboysan.consensus.configuration.MetricsConfig;
import com.mboysan.consensus.util.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class MetricsAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsAggregator.class);

    private final Map<String, AggregateMeasurement> aggregatedMeasurements = new HashMap<>();
    private final TreeSet<Measurement> sampledMeasurements = new TreeSet<>();
    private final ScheduledExecutorService executor;
    private final GraphiteFileSender graphiteFileSender;

    MetricsAggregator(MetricsConfig config, GraphiteFileSender graphiteFileSender) {
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(this::dumpMeasurements, config.step(), config.step(), TimeUnit.MILLISECONDS);
        this.graphiteFileSender = graphiteFileSender;
    }

    synchronized void add(Measurement newMeasurement) {
        if (newMeasurement instanceof AggregateMeasurement aggrMeasurement) {
            final String name = newMeasurement.getName();
            AggregateMeasurement stored = aggregatedMeasurements.get(name);
            if (stored == null) {
                aggregatedMeasurements.put(name, aggrMeasurement);
            } else {
                stored.doAggregate(newMeasurement.getValue());
            }
        } else {
            sampledMeasurements.add(newMeasurement);
        }
    }

    private synchronized void dumpMeasurements() {
        Iterator<AggregateMeasurement> aggrIter = aggregatedMeasurements.values().iterator();
        while (aggrIter.hasNext()) {
            Measurement measurement = aggrIter.next();
            graphiteFileSender.send(measurement.getName(), measurement.getValue(), measurement.getTimestamp());
            aggrIter.remove();
        }
        Iterator<Measurement> msIter = sampledMeasurements.iterator();
        while (msIter.hasNext()) {
            Measurement measurement = msIter.next();
            graphiteFileSender.send(measurement.getName(), measurement.getValue(), measurement.getTimestamp());
            msIter.remove();
        }
    }

    synchronized void shutdown() {
        dumpMeasurements();
        shutdown(executor::shutdown);
        shutdown(() -> executor.awaitTermination(5000, TimeUnit.MILLISECONDS));
    }

    private static void shutdown(ThrowingRunnable toShutdown) {
        try {
            Objects.requireNonNull(toShutdown).run();
        } catch (Throwable e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
