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
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;

public class ConnectionFT {

    private String CLUSTER_NAME = "clusterName";
    protected String SERVER_IP = "NotExist";
    protected String SERVER_PORT = "15";
    private String readPreference = "primary";
    private String writeConcern = "acknowledged";

    @Test(expected = ConnectionException.class)
    public void connectionTest() throws ConnectorException {
        MongoConnector connector = new MongoConnector();
        connector.init(null);
        connector.connect(null, getClusterConfig());
        connector.close(getClusterName());
    }

    private ConnectorClusterConfig getClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(HOST.getOptionName(), SERVER_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_PORT);
        optionsNode.put(READ_PREFERENCE.getOptionName(), readPreference);
        optionsNode.put(WRITE_CONCERN.getOptionName(), writeConcern);

        return new ConnectorClusterConfig(getClusterName(), null, optionsNode);
    }

    private ClusterName getClusterName() {
        return new ClusterName(CLUSTER_NAME);
    }
}
