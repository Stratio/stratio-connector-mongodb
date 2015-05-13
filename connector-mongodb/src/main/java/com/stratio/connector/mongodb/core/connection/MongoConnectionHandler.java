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

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * MongoConnectionHandler. See {@link ConnectionHandler}
 */
public class MongoConnectionHandler extends ConnectionHandler {

    /**
     * @param configuration
     */
    public MongoConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.stratio.connector.commons.connection.ConnectionHandler#createNativeConnection(com.stratio.meta.common.security
     * .ICredentials, com.stratio.meta.common.connector.ConnectorClusterConfig)
     */
    @Override
    protected Connection<MongoClient> createNativeConnection(ICredentials credentials,
                    ConnectorClusterConfig connectorClusterConfig) throws ConnectionException {
        try {
            return new DriverConnection(credentials, connectorClusterConfig);
        } catch (MongoValidationException e) {
            throw new ConnectionException("MongoConnector validation error", e);
        }

    }
}
