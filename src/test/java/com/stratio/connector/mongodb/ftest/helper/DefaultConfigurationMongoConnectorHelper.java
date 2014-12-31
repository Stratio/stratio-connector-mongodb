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
package com.stratio.connector.mongodb.ftest.helper;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;

import java.util.HashMap;
import java.util.Map;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

public class DefaultConfigurationMongoConnectorHelper extends MongoConnectorHelper {

    private static final MongoConnectorHelper INSTANCE = new DefaultConfigurationMongoConnectorHelper();

    private String readPreference = "primary";
    private String writeConcern = "acknowledged";// TODO test different writeConcern

    /**
     * @param clusterName
     * @throws ConnectionException
     * @throws InitializationException
     * @throws CreateNativeConnectionException
     */
    public DefaultConfigurationMongoConnectorHelper() {
        super();
    }

    public static MongoConnectorHelper getInstance() {
        return INSTANCE;
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(HOST.getOptionName(), SERVER_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_PORT);
        return new ConnectorClusterConfig(clusterName, null, optionsNode);
    }

    // @Override
    // public ConnectorClusterConfig getConnectorClusterConfig() {
    // Map<String, String> optionsNode = new HashMap<>();
    // optionsNode.put(HOST.getOptionName(), SERVER_IP);
    // optionsNode.put(PORT.getOptionName(), SERVER_PORT);
    // optionsNode.put(READ_PREFERENCE.getOptionName(), readPreference); // primary,primiaryPreferred,secondary,
    // // secondaryPreferred, nearest
    // optionsNode.put(WRITE_CONCERN.getOptionName(), writeConcern);
    // return new ConnectorClusterConfig(clusterName, null, optionsNode);
    // }
}
