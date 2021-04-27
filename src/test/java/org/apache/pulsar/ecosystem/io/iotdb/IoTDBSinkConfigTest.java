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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.utils.Lists;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;

/**
 * Unit test {@link IoTDBSinkConfig}.
 */
public class IoTDBSinkConfigTest {

    /**
     * Test Case: load the configuration from an empty property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadEmptyPropertyMap() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        IoTDBSinkConfig config = IoTDBSinkConfig.load(emptyMap);
        assertNull("Host should not be set", config.getHost());
        assertNull("Port should not be set", config.getPort());
    }

    /**
     * Test Case: load the configuration from a property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        List<IoTDBSinkConfig.TimeseriesOption> timeseriesOptionList = Lists.newArrayList();
        timeseriesOptionList.add(new IoTDBSinkConfig.TimeseriesOption(
                "root.testsg.testd.tests", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)
        );
        properties.put("timeseriesOptionList",timeseriesOptionList);
        properties.put("batchSize", 2048);
        IoTDBSinkConfig config = IoTDBSinkConfig.load(properties);
        assertEquals("Mismatched MaxMessageSize : " + config.getBatchSize(),
                2048, config.getBatchSize().intValue());
        assertEquals("Mismatched Path"+config.getTimeseriesOptionList().get(0).getPath(),
                "root.testsg.testd.tests",config.getTimeseriesOptionList().get(0).getPath());
    }

    /**
     * Test Case: load the configuration from a string property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test(expected = JsonProcessingException.class)
    public void testLoadInvalidPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("port", "abcd");
        properties.put("batchSize", -200);

        IoTDBSinkConfig.load(properties);
    }

    /**
     * Test Case: validate the configuration.
     */
    @Test
    public void testValidConfiguration() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        IoTDBSinkConfig config = IoTDBSinkConfig.load(emptyMap);
        assertNull("host should not be set", config.getHost());
        assertNull("port should not be set", config.getPort());
        try {
            config.validate();
        } catch (NullPointerException npe) {
            // expected
        }
    }

}
