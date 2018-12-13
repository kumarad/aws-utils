package com.kumarad.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Kinesis stream consumer.
 */
public abstract class AbstractRecordProcessor implements IRecordProcessor {

    private static Logger logger = LoggerFactory.getLogger(AbstractRecordProcessor.class);

    private final String consumerName;
    private final MetricRegistry metricRegistry;
    private final ObjectMapper objectMapper;
    private final Meter checkpointErrorMeter;

    private Optional<Long> maxIntervalInMillisBetweenCheckpoints = Optional.empty();
    private Optional<Integer> maxRecordsProcessedBetweenCheckpoints = Optional.empty();

    private CheckpointThresholdTracker checkpointThresholdTracker;
    private String shardId;

    public AbstractRecordProcessor(String consumerName, MetricRegistry metricRegistry, ObjectMapper objectMapper) {
        this.metricRegistry = metricRegistry;
        this.consumerName = consumerName;
        this.objectMapper = objectMapper;

        checkpointErrorMeter = metricRegistry.meter(MetricRegistry.name(getClass().getSimpleName(), consumerName, "checkpoint", "error"));
    }

    public AbstractRecordProcessor withMaxIntervalInMillisBetweenCheckpoints(long maxIntervalInMillisBetweenCheckpoints) {
        this.maxIntervalInMillisBetweenCheckpoints = Optional.of(maxIntervalInMillisBetweenCheckpoints);
        return this;
    }

    public AbstractRecordProcessor withMaxRecordsProcessedBetweenCheckpoints(int maxRecordsProcessedBetweenCheckpoints) {
        this.maxRecordsProcessedBetweenCheckpoints = Optional.of(maxRecordsProcessedBetweenCheckpoints);
        return this;
    }

    protected abstract void handleRecords(List<Record> records);
    protected abstract void shutdown();

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();
        logger.info("Initializing Kinesis Record Processor for shard {}", shardId);

        checkpointThresholdTracker = new CheckpointThresholdTracker(maxIntervalInMillisBetweenCheckpoints,
                                                                    maxRecordsProcessedBetweenCheckpoints,
                                                                    consumerName,
                                                                    metricRegistry);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        handleRecords(processRecordsInput.getRecords());
        checkpointThresholdTracker.recordsConsumed(processRecordsInput.getRecords().size());
        checkpoint(processRecordsInput.getCheckpointer());
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        logger.info("Shutting down Kinesis Record Processor for shard {}", shardId);

        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            // The checkpoint is necessary to allow a different consumer the ability to take over for this shard.
            checkpoint(shutdownInput.getCheckpointer());
        }

        shutdown();
    }

    protected String getRecordDataAsString(Record record) {
        if (record.getData() != null) {
            try {
                return objectMapper.convertValue(new String(record.getData().array()), String.class);
            } catch (Exception e) {
                return String.format("Error de-serializing record data. Error: %s", e.getMessage());
            }
        } else {
            return "<Record data is null>";
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        if (!checkpointThresholdTracker.shouldCheckpoint()) {
            return;
        }

        try {
            checkpointer.checkpoint();
            checkpointThresholdTracker.checkpoint();
        } catch (Exception e) {
            // Not going to retry. We will just try the next time we read records and that should be sufficient.
            logger.error(String.format("Failed to checkpoint for shard %s", shardId), e);
            checkpointErrorMeter.mark();
        }
    }
}