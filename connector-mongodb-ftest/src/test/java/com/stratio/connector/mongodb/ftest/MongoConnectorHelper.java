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
package com.stratio.connector.mongodb.ftest;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.metadata.ColumnType;

/**
 * @author darroyo
 *
 */
public class MongoConnectorHelper implements IConnectorHelper {

    private MongoConnectionHandler connectorHandler;
    protected String SERVER_IP = "10.200.0.62";// "10.200.0.58,10.200.0.59,10.200.0.60";
    private String SERVER_PORT = "27200";// TODO config test "9300,9300,9300";
    private String readPreference = "primaryPreferred";
    private String writeConcern = "acknowledged";// TODO test different writeConcern

    private MongoClient mongoClient;
    private ClusterName clusterName;

    public MongoConnectorHelper(ClusterName clusterName) throws ConnectionException, InitializationException,
                    CreateNativeConnectionException {
        super();
        this.clusterName = clusterName;
        MongoClientConfiguration clientConfig = new MongoClientConfiguration(getConnectorClusterConfig());

        mongoClient = new MongoClient(clientConfig.getSeeds(), clientConfig.getMongoClientOptions());

    }

    @Override
    public IConnector getConnector() {
        return new MongoConnector();
    }

    @Override
    public IConfiguration getConfiguration() {
        // return null;
        return mock(IConfiguration.class);
    }

    @Override
    public ConnectorClusterConfig getConnectorClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(HOST.getOptionName(), SERVER_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_PORT);
        optionsNode.put(READ_PREFERENCE.getOptionName(), readPreference); // primary,primiaryPreferred,secondary,
                                                                          // secondaryPreferred, nearest
        optionsNode.put(WRITE_CONCERN.getOptionName(), writeConcern);
        return new ConnectorClusterConfig(clusterName, optionsNode);
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
    @Override
    public Map recoveredCatalogSettings(String catalog) {
        // TODO Afecta a ES, no a mongoDB??
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.helper.IConnectorHelper#getAllSupportedColumnType()
     */
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

	@Override
	public boolean isCatalogMandatory() {
		// TODO Auto-generated method stub
		return true;
	}

}
