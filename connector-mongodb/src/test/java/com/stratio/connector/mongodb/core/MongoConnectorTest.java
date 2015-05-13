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

package com.stratio.connector.mongodb.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.mongodb.core.connection.DriverConnection;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.security.ICredentials;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DriverConnection.class, MongoConnector.class })
public class MongoConnectorTest {
    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String CLUSTER_NAME_OTHER = "OTHER_CLUSTER_NAME";

    @Test
    public void connectTest() throws Exception {
        MongoConnector mongoConnector = new MongoConnector();
        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, null, options);
        MongoConnectionHandler connectionHandler = mock(MongoConnectionHandler.class);
        Whitebox.setInternalState(mongoConnector, "connectionHandler", connectionHandler);
        mongoConnector.connect(iCredentials, config);
        verify(connectionHandler, times(1)).createConnection(iCredentials, config);
    }

    @Test
    public void multipleConnectTest() throws Exception {
        MongoConnector mongoConnector = new MongoConnector();
        ICredentials iCredentials = mock(ICredentials.class);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        ClusterName clusterNameOther = new ClusterName(CLUSTER_NAME_OTHER);
        Map<String, String> options = new HashMap<>();
        ConnectorClusterConfig configCluster1 = new ConnectorClusterConfig(clusterName, null, options);
        ConnectorClusterConfig configCluster2 = new ConnectorClusterConfig(clusterNameOther, null, options);
        MongoConnectionHandler connectionHandler = mock(MongoConnectionHandler.class);
        Whitebox.setInternalState(mongoConnector, "connectionHandler", connectionHandler);
        mongoConnector.connect(iCredentials, configCluster1);
        mongoConnector.connect(iCredentials, configCluster2);

        verify(connectionHandler, times(1)).createConnection(iCredentials, configCluster1);
        verify(connectionHandler, times(1)).createConnection(iCredentials, configCluster2);
    }

    @Test
    public void connectorNameTest() throws InitializationException {
        MongoConnector mongoConnector = new MongoConnector();
        if (isEmpty(mongoConnector.getConnectorName())) {
            Assert.fail("Connector name is empty");
        }
    }

    @Test
    public void connectorDatastoreNameTest() throws InitializationException {
        MongoConnector mongoConnector = new MongoConnector();
        if (isEmpty(mongoConnector.getDatastoreName())) {
            Assert.fail("Datastore name is empty");
        }
    }

    private boolean isEmpty(String... strings) {
        boolean empty = false;
        int length = strings.length;
        int i = 0;
        do {
            empty = (strings[i] == null || strings[i].trim().isEmpty());
        } while (!empty && ++i < length);
        return empty;
    }

}