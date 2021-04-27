/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.iotdb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import com.sun.xml.internal.bind.v2.TODO;

/**
 * Pulsar sink for Apache IoTDB.
 */
@Getter(AccessLevel.PACKAGE)
@Slf4j
public class IoTDBSink implements Sink<byte[]> {
    private IoTDBSinkConfig config;
    private transient SessionPool pool;
    private transient ScheduledExecutorService scheduledExecutor;

    private int batchSize = 0;
    private int flushIntervalMs = 3000;
    private int sessionPoolSize = 2;
    private List<Record<byte[]>> incomingList;

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        if (null != config) {
            log.warn("Connector is already open");
        }
        this.config = IoTDBSinkConfig.load(map);
        this.config.validate();
        batchSize = config.getBatchSize();
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
    public void write(Record record) throws Exception {
        int currentSize;
        synchronized (this.incomingList){
            if(null!=record){
                incomingList.add(record);
            }
            currentSize = incomingList.size();
        }

        if(currentSize>=batchSize){
            try{
                scheduledExecutor.submit(this::flush);
            }catch (Exception e){
                log.error("flush error",e);
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
        if(null!=scheduledExecutor){
            scheduledExecutor.shutdown();
        }
    }

    private void flush(){

    }
}
