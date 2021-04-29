/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.iotdb;


import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.mockito.Mockito;
import org.apache.commons.compress.utils.Lists;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchema;
import org.apache.pulsar.functions.api.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;


import lombok.Data;

/**
 * Test for IoTDBSink.
 */
public class IoTDBSinkTest {
    @Data
    public static class Event {
        private long timestamp;
        private String device;
        private Map<String, TSDataType> types;
        private Map<String, Object> values;
    }

    private Event event;
    IoTDBSink iotDBSink;
    private Map<String, Object> map;

    @Before
    public void setUp() throws Exception {
        event = new Event();
        event.setDevice("root.testPulsar.testd.tests");
        event.setTimestamp(Instant.now().toEpochMilli());
        event.types = Maps.newHashMap();
        event.types.put("pressure", TSDataType.DOUBLE);
        event.values = Maps.newHashMap();
        event.values.put("pressure", 40.5);

        map = new HashMap<String, Object>();
        map.put("host", "127.0.0.1");
        map.put("port", 6667);
        map.put("user", "root");
        map.put("password", "root");
        map.put("batchSize", 2);
        map.put("storageGroup", "root.testPulsar");
        List<IoTDBSinkConfig.TimeseriesOption> timeseriesOptionList = Lists.newArrayList();
        timeseriesOptionList.add(new IoTDBSinkConfig.TimeseriesOption(
                "root.testPulsar.testd.tests", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)
        );
        map.put("timeseriesOptionList", timeseriesOptionList);
        iotDBSink = new IoTDBSink();
        iotDBSink.open(map, null);
    }

    @After
    public void tearDown() throws Exception {
        iotDBSink.close();
    }

    @Test
    public void testJsonSchema() {
        JSONSchema<Event> schema = JSONSchema.of(Event.class);
        GenericSchema genericSchema = GenericSchema.of(schema.getSchemaInfo());
        byte[] bytes = schema.encode(event);
        GenericRecord record = genericSchema.decode(bytes);

        assertEquals(record.getField("device"), "test_device");
        assertEquals(((GenericRecord) record.getField("values")).getField("pressure"), 36.5);

    }

    @Test
    public void testSingleWrite() throws Exception {
        JSONSchema<Event> schema = JSONSchema.of(Event.class);
        GenericSchema genericSchema = GenericSchema.of(schema.getSchemaInfo());

        Message<GenericRecord> message = Mockito.mock(MessageImpl.class);

        Mockito.when(message.getValue()).thenReturn(genericSchema.decode(schema.encode(event)));
        Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
                .message(message)
                .topicName("test_iotdbsink")
                .build();
        iotDBSink.write(record);
    }

    @Test
    public void testMultiWrite() throws Exception {
        JSONSchema<Event> schema = JSONSchema.of(Event.class);
        GenericSchema genericSchema = GenericSchema.of(schema.getSchemaInfo());


        int testSize = 10000;
        while (testSize-- > 0) {
            event = new Event();
            event.setDevice("root.testPulsar.testd.tests");
            event.setTimestamp(Instant.now().toEpochMilli());
            event.types = Maps.newHashMap();
            event.types.put("pressure", TSDataType.DOUBLE);
            event.values = Maps.newHashMap();
            event.values.put("pressure", new Random().nextDouble());
            Message<GenericRecord> message = Mockito.mock(MessageImpl.class);

            Mockito.when(message.getValue()).thenReturn(genericSchema.decode(schema.encode(event)));
            Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
                    .message(message)
                    .topicName("test_iotdbsink")
                    .build();
            iotDBSink.write(record);
            Thread.sleep(1);
        }
    }
}
