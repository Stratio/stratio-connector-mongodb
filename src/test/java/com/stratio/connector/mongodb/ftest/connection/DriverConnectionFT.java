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

package com.stratio.connector.mongodb.ftest.connection;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.MongoClient;
import com.stratio.connector.mongodb.core.connection.DriverConnection;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;

@RunWith(PowerMockRunner.class)
public class DriverConnectionFT {

    @Mock
    MongoClient client;
    DriverConnection driverConnection;

    @Before
    public void before() throws Exception {
        ICredentials credentials = null;

        Map<String, String> options = new HashMap<>();
        options.put(HOST.getOptionName(), "10.200.0.58");
        options.put(PORT.getOptionName(), "27100");
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), null,
                        options);
        driverConnection = new DriverConnection(credentials, configuration);

    }

    @Test
    public void initialStateTest() throws Exception {
        assertNotNull("The connection is null", Whitebox.getInternalState(driverConnection, "mongoClient"));
        assertTrue("The connection is not connected", driverConnection.isConnected());
    }

    @Test
    public void withoutCredentialsTest() throws MongoValidationException {

        ICredentials credentials = mock(ICredentials.class);

        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig configuration = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), null,
                        options);

        try {
            driverConnection = new DriverConnection(credentials, configuration);
            fail("Credentials should not be accepted");
        } catch (ConnectionException e) {
        }

    }

    @Test
    public void closeTest() throws Exception {

        Whitebox.setInternalState(driverConnection, "mongoClient", client);
        driverConnection.close();

        verify(client, times(1)).close();
        assertNull("The connection should be null", Whitebox.getInternalState(driverConnection, "mongoClient"));
        assertFalse("The connection has not been closed", driverConnection.isConnected());
    }

}
