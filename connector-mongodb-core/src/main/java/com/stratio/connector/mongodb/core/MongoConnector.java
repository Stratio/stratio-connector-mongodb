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

package com.stratio.connector.mongodb.core;

import com.stratio.connector.commons.CommonsConnector;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.MongoMetadataEngine;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.exceptions.UnsupportedException;

/**
 * This class implements the connector for Mongo. Created by darroyo on 8/07/14.
 */
public class MongoConnector extends CommonsConnector {

    /**
     * Create a connection to Mongo.
     *
     * @param configuration
     *            the connection configuration. It must be not null.
     * 
     */
    @Override
    public void init(IConfiguration configuration) {
        connectionHandler = new MongoConnectionHandler(configuration);
    }

    /**
     * Return the StorageEngine.
     * 
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {
        return new MongoStorageEngine((MongoConnectionHandler) connectionHandler);

    }

    /**
     * Return the QueryEngine.
     * 
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {
        return new MongoQueryEngine((MongoConnectionHandler) connectionHandler);
    }

    /**
     * Return the MetadataEngine.
     * 
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new MongoMetadataEngine((MongoConnectionHandler) connectionHandler);
    }

    /**
     * Return the Connector Name.
     * 
     * @return Connector Name
     */
    @Override
    public String getConnectorName() {
        return "Mongo";
    }

    /**
     * Return the DataStore Name.
     * 
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[] { "Mongo" };
    }

}
