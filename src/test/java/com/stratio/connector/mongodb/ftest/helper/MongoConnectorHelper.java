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

import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.security.ICredentials;

public abstract class MongoConnectorHelper implements IConnectorHelper {

    protected String SERVER_IP = "10.200.0.58";// "10.200.0.58,10.200.0.59,10.200.0.60";
    protected String SERVER_PORT = "27100";// TODO config test "9300,9300,9300";
    private MongoClient mongoClient;
    protected ClusterName clusterName = new ClusterName("deafult_test_cluster");

    protected MongoConnectorHelper() {
        super();

        String serverIP = System.getProperty("SERVER_IP");
        if (serverIP != null) {
            SERVER_IP = serverIP;
        }
        String serverPort = System.getProperty("SERVER_PORT");
        if (serverPort != null) {
            SERVER_PORT = serverPort;
        }

        MongoClientConfiguration clientConfig = new MongoClientConfiguration(getConnectorClusterConfig());

        try {
            mongoClient = new MongoClient(clientConfig.getSeeds(), clientConfig.getMongoClientOptions());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Connection cannot be established to run functional test");
        }

    }

    @Override
    public IConnector getConnector() {
        try {
            return new MongoConnector();
        } catch (InitializationException e) {
            // TODO Auto-generated catch block
            Assert.fail("Cannot retrieve the connector");
            return null;
        }

    }

    @Override
    public IConfiguration getConfiguration() {
        // return null;
        return mock(IConfiguration.class);
    }

    @Override
    public ICredentials getICredentials() {
        return null;
        // TODO return mock(ICredentials.class);
    }

    @Override
    public void refresh(String schema) {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.helper.IConnectorHelper#recoveredCatalogSettings(java.lang.String)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Map recoveredCatalogSettings(String catalog) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.helper.IConnectorHelper#getAllSupportedColumnType()
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Collection getAllSupportedColumnType() {
        Set<ColumnType> allColumntTypes = new HashSet<>();
        // TODO new types??
        allColumntTypes.add(ColumnType.BIGINT);
        allColumntTypes.add(ColumnType.BOOLEAN);
        allColumntTypes.add(ColumnType.DOUBLE);
        allColumntTypes.add(ColumnType.FLOAT);
        allColumntTypes.add(ColumnType.INT);
        allColumntTypes.add(ColumnType.TEXT);
        allColumntTypes.add(ColumnType.VARCHAR);
        return allColumntTypes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.helper.IConnectorHelper#containIndexes(java.lang.String,
     * java.lang.String, com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    public boolean containsIndex(String catalogName, String collectionName, String indexName) {

        List<DBObject> indexInfo = mongoClient.getDB(catalogName).getCollection(collectionName).getIndexInfo();
        Iterator<DBObject> indexIterator = indexInfo.iterator();
        boolean isFound = false;
        DBObject indexObject;
        while (indexIterator.hasNext() && !isFound) {
            indexObject = indexIterator.next();
            isFound = indexName.equals(indexObject.get("name"));
        }

        return isFound;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.helper.IConnectorHelper#containIndexes(java.lang.String,
     * java.lang.String, com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    public int countIndexes(String catalogName, String collectionName) {

        List<DBObject> indexInfo = mongoClient.getDB(catalogName).getCollection(collectionName).getIndexInfo();
        return indexInfo.size();
    }

    /**
     * If create the catalog is necessary to test some functionality (as PKs...)
     */
    @Override
    public boolean isCatalogMandatory() {
        return false;
    }

    @Override
    public boolean isTableMandatory() {
        return false;
    }

    @Override
    public boolean isIndexMandatory() {
        return false;
    }

    @Override
    public boolean isPKMandatory() {
        return false;
    }

}
