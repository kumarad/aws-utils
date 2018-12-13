package com.kumarad.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ConcurrentRecordProcessorTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private AtomicInteger recordsRead;
    private Set<String> uniqueThreadsSpawned;

    @Before
    public void setup() {
        recordsRead = new AtomicInteger(0);
        uniqueThreadsSpawned = Sets.newConcurrentHashSet();
        processor.initialize(new InitializationInput().withShardId("shardId"));
    }

    @Test
    public void testMoreRecordsThanThreads() throws Exception {
        Payload payload0 = new Payload("payload0", 0);
        Payload payload1 = new Payload("payload1", 1);
        Payload payload2 = new Payload("payload2", 2);
        Payload payload3 = new Payload("payload3", 3);
        Payload payload4 = new Payload("payload4", 4);

        Record record0 = createRecord(payload0);
        Record record1 = createRecord(payload1);
        Record record2 = createRecord(payload2);
        Record record3 = createRecord(payload3);
        Record record4 = createRecord(payload4);

        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput();
        processRecordsInput.withRecords(Lists.newArrayList(record0, record1, record2, record3, record4));
        processor.processRecords(processRecordsInput);

        assertEquals(5, recordsRead.get());
        assertEquals(4, uniqueThreadsSpawned.size());
    }

    @Test
    public void testLessRecordsThanThreads() throws Exception {
        Payload payload0 = new Payload("payload0", 0);
        Payload payload1 = new Payload("payload1", 1);

        Record record0 = createRecord(payload0);
        Record record1 = createRecord(payload1);

        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput();
        processRecordsInput.withRecords(Lists.newArrayList(record0, record1));
        processor.processRecords(processRecordsInput);

        assertEquals(2, recordsRead.get());
        assertEquals(2, uniqueThreadsSpawned.size());

    }


    private Record createRecord(Payload payload) throws Exception {
        return new Record().withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(payload)))
                .withPartitionKey(Integer.toString(payload.payloadIndex));
    }

    private static class Payload {
        private String payloadString;
        private int payloadIndex;

        public Payload() {}

        public Payload(String payloadString, int payloadIndex) {
            this.payloadString = payloadString;
            this.payloadIndex = payloadIndex;
        }

        public String getPayloadString() {
            return payloadString;
        }

        public void setPayloadString(String payloadString) {
            this.payloadString = payloadString;
        }

        public int getPayloadIndex() {
            return payloadIndex;
        }

        public void setPayloadIndex(int payloadIndex) {
            this.payloadIndex = payloadIndex;
        }
    }

    private ConcurrentRecordProcessor processor = new ConcurrentRecordProcessor("consumer",
                                                                                4,
                                                                                new MetricRegistry(),
                                                                                new ObjectMapper()) {
        @Override
        protected void handleRecord(Record record) {
            try {
                Payload payload = objectMapper.readValue(record.getData().array(), Payload.class);
                String threadName = Thread.currentThread().getName();
                int threadIndex = payload.getPayloadIndex() % 4;

                if (threadName.equals(String.format("consumer-ConcurrentRecordProcessor-%s", threadIndex))) {
                    recordsRead.incrementAndGet();
                }

                uniqueThreadsSpawned.add(threadName);
            } catch (Exception e) {

            }
        }
    };
}
