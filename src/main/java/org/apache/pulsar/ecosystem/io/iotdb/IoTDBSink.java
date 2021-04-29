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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;


/**
 * Pulsar sink for Apache IoTDB.
 */

@Getter(AccessLevel.PACKAGE)
@Slf4j
public class IoTDBSink implements Sink<GenericRecord> {
    private IoTDBSinkConfig config;
    private transient SessionPool pool;
    private transient ScheduledExecutorService scheduledExecutor;

    private int batchSize = 100;
    private int flushIntervalMs = 3000;
    private int sessionPoolSize = 2;
    private List<Record<GenericRecord>> incomingList;

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        if (null != config) {
            log.warn("Connector is already open");
        }
        this.config = IoTDBSinkConfig.load(map);
        this.config.validate();
        batchSize = config.getBatchSize();
        flushIntervalMs = config.getFlushIntervalMs();
        sessionPoolSize = config.getSessionPoolSize();
        // Initiate the session poll
        pool = new SessionPool(
                config.getHost(),
                config.getPort(),
                config.getUser(),
                config.getPassword(),
                2
        );
        try {
            pool.setStorageGroup(config.getStorageGroup());
        } catch (StatementExecutionException e) {
            if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
                log.warn("Set storage group error");
            }
        }
        for (IoTDBSinkConfig.TimeseriesOption option : config.getTimeseriesOptionList()) {
            if (!pool.checkTimeseriesExists(option.getPath())) {
                try {
                    pool.createTimeseries(
                            option.getPath(), option.getDataType(), option.getEncoding(), option.getCompressor());
                } catch (Exception e) {
                    log.error("Error when create time series", e);
                }
            }
        }
        this.incomingList = new ArrayList<>();
        // Initiate scheduler
        if (batchSize > 0) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutor.scheduleAtFixedRate(
                    () -> {
                        try {
                            flush();
                        } catch (Exception e) {
                            log.error("flush error", e);
                        }
                    }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        int currentSize;
        synchronized (this.incomingList) {
            if (null != record) {
                incomingList.add(record);
            }
            currentSize = incomingList.size();
        }

        if (currentSize >= batchSize) {
            try {
                scheduledExecutor.submit(this::flush);
            } catch (Exception e) {
                log.error("flush error", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != pool) {
            try {
                flush();
            } catch (Exception e) {
                log.error("flush error", e);
            }
            pool.close();
        }
        if (null != scheduledExecutor) {
            scheduledExecutor.shutdown();
        }
    }

    private void flush() {
        List<Record<GenericRecord>> toFlushList;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            toFlushList = incomingList;
            incomingList = Lists.newArrayList();
        }
        List<String> deviceIds = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        for (Record<GenericRecord> record : toFlushList) {
            deviceIds.add((String) record.getValue().getField("device"));
            timestamps.add(Long.valueOf((String) record.getValue().getField("timestamp")));
            List<String> recordMeasurements = new ArrayList<>();
            List<Object> recordValues = new ArrayList<>();
            List<TSDataType> recordType = new ArrayList<>();
            for (Field field : ((GenericRecord) record.getValue().getField("values")).getFields()) {
                String measurement = field.getName();
                recordMeasurements.add(measurement);
                recordValues.add(((GenericRecord) record.getValue().getField("values")).getField(measurement));
                String typeString = ((GenericRecord) record.getValue().getField("types"))
                        .getField(measurement).toString();
                recordType.add(getType(typeString));
            }
            typesList.add(recordType);
            measurementsList.add(recordMeasurements);
            valuesList.add(recordValues);
        }
        try {
            pool.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
            toFlushList.clear();
        } catch (Exception e) {
            log.error("Insert error", e);
            toFlushList.forEach(Record::fail);
        }

    }

    private TSDataType getType(String type) throws SchemaSerializationException {
        if (type.equals("DOUBLE")) {
            return TSDataType.DOUBLE;
        } else if (type.equals("FLOAT")) {
            return TSDataType.FLOAT;
        } else if (type.equals("BOOLEAN")) {
            return TSDataType.BOOLEAN;
        } else if (type.equals("INT32")) {
            return TSDataType.INT32;
        } else if (type.equals("INT64")) {
            return TSDataType.INT64;
        } else if (type.equals("TEXT")) {
            return TSDataType.TEXT;
        } else {
            log.error("Unknown value type" + type);
            throw new SchemaSerializationException("Unknown value type" + type);
        }
    }
}
