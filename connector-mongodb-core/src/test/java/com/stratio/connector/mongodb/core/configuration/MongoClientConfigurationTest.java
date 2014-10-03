/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.connector.mongodb.core.configuration;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.MAX_CONNECTIONS_PER_HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta2.common.data.ClusterName;

/**
 * The Test for the configuration for Mongo connector.
 */

public class MongoClientConfigurationTest {

    private final static ClusterName CLUSTER_NAME = new ClusterName("clusterMongo");
    private final static String SERVER_IP = "125.0.1.1";
    private final static String SERVER_PORT = "12";
    private final static String CUSTOM_READ_PREFERENCE = "secondaryPreferred";
    private final static String CUSTOM_WRITE_PREFERENCE = "unacknowledged";
    private final static String CUSTOM_MAX_CONNECTION = "5000";

    @Test
    public void testDefaultConfiguration() throws CreateNativeConnectionException {
        Map<String, String> properties = null;
        ConnectorClusterConfig defaultConfig = new ConnectorClusterConfig(CLUSTER_NAME, properties);
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
    public void testCustomConfiguration() throws CreateNativeConnectionException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HOST.getOptionName(), SERVER_IP);
        properties.put(PORT.getOptionName(), SERVER_PORT);
        properties.put(READ_PREFERENCE.getOptionName(), CUSTOM_READ_PREFERENCE);
        properties.put(WRITE_CONCERN.getOptionName(), CUSTOM_WRITE_PREFERENCE);
        properties.put(MAX_CONNECTIONS_PER_HOST.getOptionName(), CUSTOM_MAX_CONNECTION);

        ConnectorClusterConfig defaultConfig = new ConnectorClusterConfig(CLUSTER_NAME, properties);
        MongoClientConfiguration config = new MongoClientConfiguration(defaultConfig);

        // Review the default options in Mongo
        assertEquals(config.getMongoClientOptions().getConnectionsPerHost(), Integer.parseInt(CUSTOM_MAX_CONNECTION));
        assertEquals(config.getMongoClientOptions().getReadPreference(), ReadPreference.secondaryPreferred());
        assertEquals(config.getMongoClientOptions().getWriteConcern(), WriteConcern.UNACKNOWLEDGED);

        // Connector properties
        assertTrue(config.getSeeds().size() == 1);
        assertEquals(config.getSeeds().get(0).getPort(), Integer.parseInt(SERVER_PORT));

    }

}
