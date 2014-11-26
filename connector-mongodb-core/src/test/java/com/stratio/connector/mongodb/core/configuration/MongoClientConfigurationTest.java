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

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.MAX_CONNECTIONS_PER_HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;

/**
 * The Test for the configuration for Mongo connector.
 */

public class MongoClientConfigurationTest {

    private final static ClusterName CLUSTER_NAME = new ClusterName("clusterMongo");
    private final static String SERVER_IP = "125.0.1.1";
    private final static String SERVER_PORT = "12";
    private final static String SERVER_IP_LIST = "125.0.1.1, 127.0.1.1";
    private final static String SERVER_PORT_LIST = "2700, 2701";
    private final static String CUSTOM_READ_PREFERENCE = "secondaryPreferred";
    private final static String CUSTOM_WRITE_PREFERENCE = "unacknowledged";
    private final static String CUSTOM_MAX_CONNECTION = "5000";

    @Test
    public void defaultConfigurationTest() throws CreateNativeConnectionException, MongoValidationException {
        Map<String, String> properties = null;
        ConnectorClusterConfig defaultConfig = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(defaultConfig);

        // Review the default options in Mongo
        assertEquals(config.getMongoClientOptions().getAcceptableLatencyDifference(), 15);
        assertEquals(config.getMongoClientOptions().getConnectionsPerHost(), 10000);
        assertEquals(config.getMongoClientOptions().getMaxConnectionIdleTime(), 0);
        assertEquals(config.getMongoClientOptions().getConnectTimeout(), 10000);
        assertEquals(config.getMongoClientOptions().getReadPreference(), ReadPreference.primaryPreferred());
        assertEquals(config.getMongoClientOptions().getWriteConcern(), WriteConcern.ACKNOWLEDGED);

        // Connector properties
        assertTrue(config.getSeeds().size() == 1);
        assertEquals(config.getSeeds().get(0).getPort(), 27017);

    }

    @Test
    public void customConfigurationTest() throws CreateNativeConnectionException, NumberFormatException,
                    MongoValidationException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HOST.getOptionName(), SERVER_IP);
        properties.put(PORT.getOptionName(), SERVER_PORT);
        properties.put(READ_PREFERENCE.getOptionName(), CUSTOM_READ_PREFERENCE);
        properties.put(WRITE_CONCERN.getOptionName(), CUSTOM_WRITE_PREFERENCE);
        properties.put(MAX_CONNECTIONS_PER_HOST.getOptionName(), CUSTOM_MAX_CONNECTION);

        ConnectorClusterConfig customConfi = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(customConfi);

        // Review the default options in Mongo
        assertEquals(config.getMongoClientOptions().getConnectionsPerHost(), Integer.parseInt(CUSTOM_MAX_CONNECTION));
        assertEquals(config.getMongoClientOptions().getReadPreference(), ReadPreference.secondaryPreferred());
        assertEquals(config.getMongoClientOptions().getWriteConcern(), WriteConcern.UNACKNOWLEDGED);

        // Connector properties
        assertTrue(config.getSeeds().size() == 1);
        assertEquals(config.getSeeds().get(0).getPort(), Integer.parseInt(SERVER_PORT));

    }

    @Test
    public void wrongReadPreferenceTest() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(READ_PREFERENCE.getOptionName(), "falseReadPreference");

        ConnectorClusterConfig wrongConfig = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);

        MongoClientConfiguration config = new MongoClientConfiguration(wrongConfig);
        try {
            config.getMongoClientOptions();
            fail("A exception must be thrown when no match any option");
        } catch (MongoValidationException e) {
        }

    }

    @Test
    public void wrongIntegerPreferenceTest() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(MAX_CONNECTIONS_PER_HOST.getOptionName(), "ten");

        ConnectorClusterConfig wrongConfig = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(wrongConfig);
        try {
            config.getMongoClientOptions();
            fail("An exception must be thrown when a non integer received");
        } catch (MongoValidationException e) {
        }

    }

    @Test
    public void multipleHostsTest() throws CreateNativeConnectionException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HOST.getOptionName(), SERVER_IP_LIST);
        properties.put(PORT.getOptionName(), SERVER_PORT_LIST);

        ConnectorClusterConfig wrongConfig = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(wrongConfig);

        assertTrue(config.getSeeds().size() == 2);

        assertEquals(2700, config.getSeeds().get(0).getPort());
        assertEquals(2701, config.getSeeds().get(1).getPort());

    }

    @Test
    public void wrongSeedsTest() throws CreateNativeConnectionException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HOST.getOptionName(), SERVER_IP_LIST);
        properties.put(PORT.getOptionName(), SERVER_PORT);

        ConnectorClusterConfig wrongConfig = new ConnectorClusterConfig(CLUSTER_NAME, null, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(wrongConfig);
        try {
            config.getSeeds();
            fail("An exception must be thrown when number of ports and hosts are different");
        } catch (CreateNativeConnectionException e) {
        }

    }

}
