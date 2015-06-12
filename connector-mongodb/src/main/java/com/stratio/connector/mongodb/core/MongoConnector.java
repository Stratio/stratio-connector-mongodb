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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.commons.util.ManifestUtil;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.MongoMetadataEngine;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.connectors.ConnectorApp;

/**
 * This class implements the connector for Mongo.
 */
public class MongoConnector extends CommonsConnector {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    /**
     * Instantiates a new mongo connector.
     *
     * @throws InitializationException
     *             if any error exists during the initialization
     */
    public MongoConnector() throws InitializationException {
        super("/MongoConnector.xml", "/MongoDataStore.xml");

    }

    /**
     * Create a connection to Mongo.
     *
     * @param configuration
     *            the connection configuration. It must be not null
     */
    @Override
    public void init(IConfiguration configuration) {
        connectionHandler = new MongoConnectionHandler(configuration);
    }

    /**
     * REstar the connector.
     * @throws ExecutionException if an exception happens.
     */
    @Override
    public void restart() throws ExecutionException {

    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {
        return MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
    }

    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {
        return MongoQueryEngine.getInstance((MongoConnectionHandler) connectionHandler);

   }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     * 
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return MongoMetadataEngine.getInstance((MongoConnectionHandler) connectionHandler);

    }


    /**
     * The main method.
     *
     * @param args
     *            the arguments
     * @throws InitializationException
     *             if any error exists during the initialization
     */
    public static void main(String[] args) throws InitializationException {

        MongoConnector mongoConnector = new MongoConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(mongoConnector);
        mongoConnector.attachShutDownHook();
    }

    /**
     * Attach shut down hook.
     */
    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdown();
                } catch (ExecutionException e) {
                    logger.error("Shutdown connector: " + e.getMessage(), e);
                }
            }
        });
    }

}
