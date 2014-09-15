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

import java.util.HashMap;
import java.util.Map;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.elasticsearch.ftest.helper.IConnectorHelper;
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

import static org.mockito.Mockito.mock;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.*;
/**
 * @author darroyo
 *
 */
public class MongoConnectorHelper implements IConnectorHelper{

	private MongoConnectionHandler connectorHandle;
	protected String SERVER_IP = "localhost";//"10.200.0.58,10.200.0.59,10.200.0.60";
	private String SERVER_PORT = "27017";//TODO config test "9300,9300,9300";
	private String readPreference = "primaryPreferred";
	private String writeConcern = "acknowledged";//TODO test different writeConcern
	
	private MongoClient mongoClient;
	private ClusterName clusterName;

	public MongoConnectorHelper(ClusterName clusterName)throws ConnectionException, InitializationException, CreateNativeConnectionException {
		super();
		this.clusterName = clusterName;
		MongoClientConfiguration clientConfig = new MongoClientConfiguration(getConnectorClusterConfig());
		
		mongoClient = new MongoClient( clientConfig.getSeeds(), clientConfig.getMongoClientOptions());
				
	}

	@Override
	public IConnector getConnector() {
		return new MongoConnector();
	}

	@Override
	public IConfiguration getConfiguration() {
		//return null;
		 return mock(IConfiguration.class);
	}

	@Override
	public ConnectorClusterConfig getConnectorClusterConfig() {
		Map<String, String> optionsNode = new HashMap<>();
		optionsNode.put(HOST.getOptionName(), SERVER_IP);
		optionsNode.put(PORT.getOptionName(), SERVER_PORT);
		optionsNode.put(READ_PREFERENCE.getOptionName(), readPreference);
		optionsNode.put(WRITE_CONCERN.getOptionName(), writeConcern);
		return new ConnectorClusterConfig(clusterName, optionsNode);
	}

	@Override
	public ICredentials getICredentials() {
		return null;
		//TODO return mock(ICredentials.class);
	}

	@Override
	public void deleteSet(String schema) {
	
			if (mongoClient != null) mongoClient.dropDatabase(schema);
				
		
	}

	@Override
	public void refresh(String schema) {
		
	}

}
