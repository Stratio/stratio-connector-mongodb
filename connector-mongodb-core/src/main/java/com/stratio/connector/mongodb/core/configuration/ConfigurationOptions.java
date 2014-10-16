/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.mongodb.core.configuration;

/**
 * @author darroyo Set of options for the mongo connector. A default value is provided.
 *
 */

public enum ConfigurationOptions {

    ACCEPTABLE_LATENCY("mongo.acceptableLatencyDifference", "15"), MAX_CONNECTIONS_PER_HOST(
                    "mongo.maxConnectionsPerHost", "10000"), MAX_IDLE_TIME("mongo.maxConnectionIdleTime", "0"), CONNECTION_TIMEOUT(
                    "mongo.connectTimeout", "10000"), READ_PREFERENCE("mongo.readPreference", "primaryPreferred"), WRITE_CONCERN(
                    "mongo.writeConcern", "acknowledged"),

    HOST("Hosts", new String[] { "localhost" }), PORT("Ports", new String[] { "27017" });

    private final String optionName;
    private final String[] defaultValue;

    public String[] getDefaultValue() {
        return defaultValue;
    }

    public String getOptionName() {
        return optionName;
    }

    ConfigurationOptions(String optionName, String... defaultValue) {
        this.optionName = optionName;
        this.defaultValue = defaultValue;
    }

}
