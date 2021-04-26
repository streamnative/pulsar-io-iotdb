/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contrexbutor license agreements.  See the NOTICE file
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

import avro.shaded.com.google.common.base.Preconditions;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;


import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
public class IoTDBSinkConfig extends IoTDBConfig implements Serializable {
    private String storageGroup;
    private Integer batchSize;
    /**
     * Validate if the configuration is valid
     */
    public void validate(){
        Preconditions.checkNotNull(getHost(), "host property not set.");
        Preconditions.checkNotNull(getPort(),"port property not set.");
        Preconditions.checkNotNull(getUser(),"user property not set.");
        Preconditions.checkNotNull(getPassword(),"password property not set");
        Preconditions.checkNotNull(getStorageGroup(), "storageGroup property not set.");
        Preconditions.checkNotNull(getBatchSize(), "batchSize property not set.");
        Preconditions.checkState(getBatchSize()>0,"batchSize property should be positive.");
    }

    public static IoTDBSinkConfig load(Map<String, Object> map)throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map),IoTDBSinkConfig.class);
    }
}
