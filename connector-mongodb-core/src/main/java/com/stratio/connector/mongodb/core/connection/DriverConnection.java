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
package com.stratio.connector.mongodb.core.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;

/**
 * @author darroyo
 *
 */
public class DriverConnection extends Connection<MongoClient> {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MongoClient mongoClient = null;
    private boolean isConnected = false;

    public DriverConnection(ICredentials credentials, ConnectorClusterConfig connectorClusterConfig)
                    throws CreateNativeConnectionException {
        MongoClientConfiguration mongoClientConfiguration = new MongoClientConfiguration(connectorClusterConfig);

        if (credentials == null) {
            mongoClient = new MongoClient(mongoClientConfiguration.getSeeds(),
                            mongoClientConfiguration.getMongoClientOptions());
            isConnected = true;
            logger.info("MongoDB connection established ");

        } else {
            throw new CreateNativeConnectionException("Credentials are not supported");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.connection.Connection#close()
     */
    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            logger.info("Disconnected from Mongo");

        }
        isConnected = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.connection.Connection#getNativeConnection()
     */
    @Override
    public MongoClient getNativeConnection() {
        return mongoClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.connection.Connection#isConnect()
     */
    @Override
    public boolean isConnect() {
        return isConnected;
    }

}
