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

import com.stratio.connector.mongodb.core.configuration.ConfigurationOptions;
import com.stratio.connector.mongodb.core.engine.metadata.DiscoverMetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.security.ICredentials;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.*;


/**
 * Implements a Mongo Connection. See {@link Connection}
 */
public class DriverConnection extends Connection<MongoClient> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MongoClient mongoClient = null;

    /**
     * Instantiates a new driver connection.
     *
     * @param credentials
     *            the credentials
     * @param connectorClusterConfig
     *            the connector cluster configuration
     * @throws ConnectionException
     * @throws MongoValidationException
     *             the mongo validation exception
     */
    public DriverConnection(ICredentials credentials, ConnectorClusterConfig connectorClusterConfig)
                    throws ConnectionException, MongoValidationException {
        MongoClientConfiguration mongoClientConfiguration = new MongoClientConfiguration(connectorClusterConfig);
        String sampleProperty = DiscoverMetadataUtils.recoveredSampleProperty(connectorClusterConfig);

        String defaultLimit = DiscoverMetadataUtils.recoveredDefaultLimit(connectorClusterConfig);

        addObjectToSession(SAMPLE_PROBABILITY.getOptionName(), sampleProperty);
        addObjectToSession(DEFAULT_LIMIT.getOptionName(), defaultLimit);

        if (credentials == null) {
            mongoClient = new MongoClient(mongoClientConfiguration.getSeeds(),
                            mongoClientConfiguration.getMongoClientOptions());
            if (isConnected()) {
                logger.info("New MongoDB connection established");
            } else {
                throw new ConnectionException("Cannot connect with MongoDB");
            }

        } else {
            throw new ConnectionException("Credentials are not supported yet");
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
        }
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
    public final boolean isConnected() {

        boolean isConnected = false;

        if (mongoClient != null) {
            try {
                mongoClient.getConnectPoint();
                isConnected = true;
            } catch (Exception error) {
                logger.error("Error connecting with Mongo :" + error.getMessage(), error);
            }
        }
        return isConnected;
    }



}
