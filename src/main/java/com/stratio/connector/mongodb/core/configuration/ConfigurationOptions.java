/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
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

package com.stratio.connector.mongodb.core.configuration;

/**
 * The ConfigurationOptions. They could be set in the ConnectorClusterConfiguration Set of options for the Mongo
 * Connector. A default value is provided.
 * 
 */

public enum ConfigurationOptions {

    /** The acceptable latency. */
    ACCEPTABLE_LATENCY("mongo.acceptableLatencyDifference", "15"),
    /** The max connections per host. */
    MAX_CONNECTIONS_PER_HOST("mongo.maxConnectionsPerHost", "10000"),
    /** The max idle time. */
    MAX_IDLE_TIME("mongo.maxConnectionIdleTime", "0"),
    /** The connection timeout. */
    CONNECTION_TIMEOUT("mongo.connectTimeout", "10000"),
    /** The read preference. */
    READ_PREFERENCE("mongo.readPreference", "primaryPreferred"),
    /** The write concern. */
    WRITE_CONCERN("mongo.writeConcern", "acknowledged"),
    /** The host. */
    HOST("Hosts", new String[] { "localhost" }),
    /** The port. */
    PORT("Port", new String[] { "27017" });

    /** The option name. */
    private final String optionName;

    /** The default value. */
    private final String[] defaultValue;

    /**
     * Gets the default value.
     *
     * @return the default value
     */
    public String[] getDefaultValue() {
        return defaultValue.clone();
    }

    /**
     * Gets the option name.
     *
     * @return the option name
     */
    public String getOptionName() {
        return optionName;
    }

    /**
     * Instantiates a new configuration options.
     *
     * @param optionName
     *            the option name
     * @param defaultValue
     *            the default value
     */
    ConfigurationOptions(String optionName, String... defaultValue) {
        this.optionName = optionName;
        this.defaultValue = defaultValue;
    }

}
