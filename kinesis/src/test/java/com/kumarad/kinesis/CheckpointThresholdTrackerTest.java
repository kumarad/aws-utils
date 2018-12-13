package com.kumarad.kinesis;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CheckpointThresholdTrackerTest {

    @Test
    public void noThresholdTest() {
        CheckpointThresholdTracker tracker = new CheckpointThresholdTracker(Optional.empty(),
                                                                            Optional.empty(),
                                                                            "consumer",
                                                                            new MetricRegistry());

        assertTrue(tracker.shouldCheckpoint());
        tracker.recordsConsumed(10);
        tracker.checkpoint();
        assertTrue(tracker.shouldCheckpoint());
    }

    @Test
    public void timeOnlyThresholdTest() throws Exception {
        CheckpointThresholdTracker tracker = new CheckpointThresholdTracker(Optional.of(500L),
                                                                            Optional.empty(),
                                                                            "consumer",
                                                                            new MetricRegistry());

        Thread.sleep(100);
        // It hasn't been long enough for us to want a checkpoint.
        assertFalse(tracker.shouldCheckpoint());
        assertFalse(tracker.shouldCheckpoint());


        Thread.sleep(500);
        // By this point its been 500 millis. Should be able to checkpoint.
        assertTrue(tracker.shouldCheckpoint());

        // Since we haven't checkpointed should still let us checkpoint.
        assertTrue(tracker.shouldCheckpoint());

        // Checkpointing will reset the tracker.
        tracker.checkpoint();
        // Should not be able to checkpoint since the timer should have reset.
        assertFalse(tracker.shouldCheckpoint());
        Thread.sleep(700);

        // Its not time to checkpoint again.
        assertTrue(tracker.shouldCheckpoint());
    }

    @Test
    public void recordThresholdOnlyTest() {
        CheckpointThresholdTracker tracker = new CheckpointThresholdTracker(Optional.empty(),
                                                                            Optional.of(100),
                                                                            "consumer",
                                                                            new MetricRegistry());

        // We haven't consumed any records. So shouldn't let us checkpoint.
        assertFalse(tracker.shouldCheckpoint());

        tracker.recordsConsumed(50);
        // Still haven't hit our threshold of 100.
        assertFalse(tracker.shouldCheckpoint());

        tracker.recordsConsumed(50);

        // We should not be able to checkpoint.
        assertTrue(tracker.shouldCheckpoint());

        // Since we still haven't checkpointed, should still return true.
        assertTrue(tracker.shouldCheckpoint());

        tracker.checkpoint();

        // We should have reset due to the above checkpoint.
        assertFalse(tracker.shouldCheckpoint());

        tracker.recordsConsumed(500);

        // Past our threshold so should let us checkpoint.
        assertTrue(tracker.shouldCheckpoint());
    }

    @Test
    public void timeAndRecordThresholdTest() throws Exception {
        CheckpointThresholdTracker tracker = new CheckpointThresholdTracker(Optional.of(500L),
                                                                            Optional.of(100),
                                                                            "consumer",
                                                                            new MetricRegistry());

        tracker.recordsConsumed(500);

        // Should let us checkpoint because we have consumed more than the threshold records even though the
        // threshold interval has not been hit.
        assertTrue(tracker.shouldCheckpoint());

        tracker.checkpoint();

        Thread.sleep(200L);

        tracker.recordsConsumed(50);
        // Not enough time has passed since last checkpoint and we haven't hit record threshold either.
        assertFalse(tracker.shouldCheckpoint());

        Thread.sleep(400L);

        // Record threshold hasn't been hit but interval threshold has.
        assertTrue(tracker.shouldCheckpoint());
    }
}
