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

package com.stratio.connector.mongodb.core.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;
import java.util.Map;

import com.stratio.connector.mongodb.core.configuration.ConfigurationOptions;
import com.stratio.connector.mongodb.core.engine.metadata.DiscoverMetadataUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;

import javax.security.auth.login.Configuration;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MongoConnectionHandler.class })
public class ConnectionHandlerTest {

    private static final String CLUSTER_NAME = "cluster_name";
    private MongoConnectionHandler connectionHandler = null;
    @Mock
    private IConfiguration iConfiguration;

    @Before
    public void before() throws Exception {
        connectionHandler = new MongoConnectionHandler(iConfiguration);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void createDriverConnectionTest() throws Exception {

        ICredentials credentials = mock(ICredentials.class);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), null, options);

        DriverConnection connection = mock(DriverConnection.class);
        whenNew(DriverConnection.class).withArguments(credentials, config).thenReturn(connection);

        connectionHandler.createConnection(credentials, config);

        Map<String, Connection> mapConnection = (Map<String, Connection>) Whitebox.getInternalState(connectionHandler,
                        "connections");

        DriverConnection recoveredConnection = (DriverConnection) mapConnection.get(CLUSTER_NAME);

        assertNotNull("The connection does not exist", recoveredConnection);
        assertEquals("The recovered connection is not the expected", connection, recoveredConnection);

    }

    public ConnectorClusterConfig initConnectorOptionsWithSampleProbability(String probability){
        Map<String, String> options = new HashMap<>();
        Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put(ConfigurationOptions.SAMPLE_PROBABILITY.getOptionName(), probability);
        ConnectorClusterConfig config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), connectorOptions, null);
        return config;
    }

    @Test
    public void recoveredSamplePropertyTest() throws Exception {
        String result = "";
        ConnectorClusterConfig config = initConnectorOptionsWithSampleProbability("0.5");
        result = DiscoverMetadataUtils.recoveredSampleProperty(config);
        assertEquals( "0.5",result);
        config = initConnectorOptionsWithSampleProbability(null);
        result = DiscoverMetadataUtils.recoveredSampleProperty(config);
        assertEquals("1", result);
        config = new ConnectorClusterConfig(new ClusterName(CLUSTER_NAME), null, null);
        result = DiscoverMetadataUtils.recoveredSampleProperty(config);
        assertEquals("1", result);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void closeConnectionTest() throws Exception {

        Map<String, DriverConnection> mapConnection = (Map<String, DriverConnection>) Whitebox.getInternalState(
                        connectionHandler, "connections");
        DriverConnection connection = mock(DriverConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        connectionHandler.closeConnection(CLUSTER_NAME);

        assertFalse("The connection " + CLUSTER_NAME + " should have been closed",
                        mapConnection.containsKey(CLUSTER_NAME));
        verify(connection, times(1)).close();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void getConnectionTest() throws ExecutionException {
        Map<String, DriverConnection> mapConnection = (Map<String, DriverConnection>) Whitebox.getInternalState(
                        connectionHandler, "connections");
        DriverConnection connection = mock(DriverConnection.class);
        mapConnection.put(CLUSTER_NAME, connection);

        Connection<MongoClient> recoveredConnection = connectionHandler.getConnection(CLUSTER_NAME);

        assertNotNull("The connection does not exist", recoveredConnection);
        assertSame("The connection recovered is not the expected", connection, recoveredConnection);

    }

}
