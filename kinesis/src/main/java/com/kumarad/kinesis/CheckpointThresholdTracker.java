package com.kumarad.kinesis;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.time.Instant;
import java.util.Optional;

/**
 * Controls the frequency of checkpoints.
 */
public class CheckpointThresholdTracker {
    private final Optional<Long> maxIntervalInMillisBetweenCheckpoints;
    private final Optional<Integer> maxRecordsProcessedBetweenCheckpoints;

    private final Histogram millisBetweenCheckpoints;
    private final Histogram recordsProcessedBetweenCheckpoints;

    private Instant lastCheckpointInstant;
    private int recordsConsumedSinceLastCheckpoint;

    public CheckpointThresholdTracker(Optional<Long> maxIntervalInMillisBetweenCheckpoints,
            Optional<Integer> maxRecordsProcessedBetweenCheckpoints,
            String consumerName,
            MetricRegistry metricRegistry) {
        this.maxIntervalInMillisBetweenCheckpoints = maxIntervalInMillisBetweenCheckpoints;
        this.maxRecordsProcessedBetweenCheckpoints = maxRecordsProcessedBetweenCheckpoints;

        millisBetweenCheckpoints = metricRegistry.histogram(MetricRegistry.name(getClass().getSimpleName(), consumerName, "checkpointIntervalInMillis"));
        recordsProcessedBetweenCheckpoints = metricRegistry.histogram(MetricRegistry.name(getClass().getSimpleName(), consumerName, "recordsProcessedBetweenCheckpoints"));

        lastCheckpointInstant = Instant.now();

        // Initialize the tracker by invoking checkpoint.
        checkpoint();
    }

    /**
     * Resets the tracker. Should be called each time a checkpoint is taken.
     */
    public void checkpoint() {
        millisBetweenCheckpoints.update(Instant.now().minusMillis(lastCheckpointInstant.toEpochMilli()).toEpochMilli());
        recordsProcessedBetweenCheckpoints.update(recordsConsumedSinceLastCheckpoint);

        lastCheckpointInstant = Instant.now();
        recordsConsumedSinceLastCheckpoint = 0;
    }

    /**
     * Client is responsible for invoking this every time new records are consumed.
     */
    public void recordsConsumed(int recordsConsumed) {
        this.recordsConsumedSinceLastCheckpoint += recordsConsumed;
    }

    /**
     * If both a records processed and time threshold are provided, the records processed will be checked
     * first followed by the interval.
     * If neither are specified shouldCheckpoint will always return true.
     */
    public boolean shouldCheckpoint() {
        Optional<Boolean> recordThresholdHit = hasRecordThresholdBeenHit();
        Optional<Boolean> intervalThresholdHit = hasIntervalThresholdBeenHit();
        if (recordThresholdHit.isPresent() && intervalThresholdHit.isPresent()) {
            return recordThresholdHit.get() || intervalThresholdHit.get();
        } else return recordThresholdHit.orElseGet(() -> intervalThresholdHit.orElse(true));
    }

    private Optional<Boolean> hasRecordThresholdBeenHit() {
        return maxRecordsProcessedBetweenCheckpoints.map(threshold -> recordsConsumedSinceLastCheckpoint >= threshold);
    }

    private Optional<Boolean> hasIntervalThresholdBeenHit() {
        return maxIntervalInMillisBetweenCheckpoints.map(interval -> Instant.now().minusMillis(lastCheckpointInstant.toEpochMilli()).toEpochMilli() > interval);
    }
}
