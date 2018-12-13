package com.kumarad.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Distributes records consumption from kinesis across multiple threads.
 */
public abstract class ConcurrentRecordProcessor extends AbstractRecordProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRecordProcessor.class);

    private final String consumerName;
    private final int threadCount;
    private final MetricRegistry metricRegistry;

    private final Meter errorMeter;
    private final ExecutorService executorService;

    public ConcurrentRecordProcessor(String consumerName, int threadCount, MetricRegistry metricRegistry, ObjectMapper objectMapper) {
        super(consumerName, metricRegistry, objectMapper);
        this.consumerName = consumerName;
        this.threadCount = threadCount;
        this.metricRegistry = metricRegistry;

        errorMeter = metricRegistry.meter(MetricRegistry.name(getClass().getSimpleName(), consumerName, "error"));

        final String threadName = String.format("%s-ConcurrentRecordProcessor", consumerName);
        executorService = Executors.newFixedThreadPool(threadCount,
                                                       new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
    }

    protected abstract void handleRecord(Record record);

    @Override
    protected void handleRecords(List<Record> records) {
        // Group the records across the number of threads available for processing.
        Map<Integer, List<Record>> groups = records.stream()
                .collect(Collectors.groupingBy(r -> getThreadIndex(r.getPartitionKey())));

        // Initialize the latch based on number of actual threads that will be spawned. Could be less than threadCount.
        CountDownLatch latch = new CountDownLatch(groups.size());

        // Spawn a thread for each existing grouping.
        groups.forEach((index, group) -> {
            metricRegistry.histogram(MetricRegistry.name(getClass().getSimpleName(), consumerName,
                                                         String.format("bucket-%s", index), "records")).update(group.size());
            executorService.submit(() -> {
                try {
                    records.forEach(record -> {
                        try {
                            handleRecord(record);
                        } catch (Exception e) {
                            logger.error(String.format("Failed to process record: %s", getRecordDataAsString(record)), e);
                            errorMeter.mark();
                        }
                    });
                } finally {
                    latch.countDown();
                }
            });
        });

        // Wait for all the threads to get done processing.
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for threads to finish processing records. Giving up.");
        }
    }

    @Override
    protected void shutdown() {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 30, TimeUnit.SECONDS);
    }

    private int getThreadIndex(String partitionKey) {
        if (StringUtils.isBlank(partitionKey)) {
            return 0;
        }
        return Math.abs(partitionKey.hashCode()) % threadCount;
    }
}