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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.mongodb.core.configuration.ConnectionConfigurationCreator;
import com.stratio.connector.mongodb.core.configuration.SupportedOperationsCreator;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.MongoMetadataEngine;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;




/**
 * This class implements the connector for Mongo. Created by darroyo on 8/07/14.
 */
public class MongoConnector implements IConnector {

	/**
	* The Log.
	*/
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	 /**
	* The connectionHandler.
	*/
	private MongoConnectionHandler connectionHandler = null;

	
	/**
	* Create a connection to Mongo.
	*
	* @param configuration the connection configuration. It must be not null.
	* onnection.
	*/
	@Override
	public void init(IConfiguration configuration) {
		connectionHandler = new MongoConnectionHandler(configuration);
	}
	
	

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#connect(com.stratio.meta.common.security.ICredentials, com.stratio.meta.common.connector.ConnectorClusterConfig)
	 */
	@Override
	public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
		try {
			connectionHandler.createConnection(credentials, config);
		} catch (HandlerConnectionException e) {
			//TODO ?
			throw new ConnectionException("connection failed", e);
		}
		
	}

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#close(com.stratio.meta2.common.data.ClusterName)
	 */
	@Override
	public void close(ClusterName name) throws ConnectionException {
		connectionHandler.closeConnection(name.getName());
		
	}


	/**
	* The connection status.
	*
	* @return true if the driver's client is not null.
	*/
	@Override
	public boolean isConnected(ClusterName name) {
		connectionHandler.isConnected(name.getName());
		return false;
	}
	
	

	/**
	 * Return the StorageEngine.
	 * 
	 * @return the StorageEngine
	 */
	@Override
	public IStorageEngine getStorageEngine() {
		return new MongoStorageEngine(connectionHandler);

	}


	/**
	 * Return the QueryEngine.
	 * 
	 * @return the QueryEngine
	 */
	@Override
	public IQueryEngine getQueryEngine(){
		return new MongoQueryEngine(connectionHandler);
	}


	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#getMetadataEngine()
	 */
	@Override
	public IMetadataEngine getMetadataEngine() throws UnsupportedException {
		return new MongoMetadataEngine(connectionHandler);
	}

	
//	@Override
//	public IConnectorConfiguration getConnectorConfiguration() {
//		return null;
//	}

	/**
     * Return the supported operations
     *
     * @return the supported operations.
     */
    public Map<Operations, Boolean> getSupportededOperations() {
        return SupportedOperationsCreator.getSupportedOperations();
    }
    
    
    /**
     * Return the supported configuration options
     *
     * @return the the supported configuration options.
     */
    public Set<ConnectionConfiguration> getConnectionConfiguration(){
    	return ConnectionConfigurationCreator.getConfiguration();
    }
	

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#getConnectorName()
	 */
	@Override
	public String getConnectorName() {
	return "Mongo";
	}



	/**
	* Return the DataStore Name.
	* @return DataStore Name
	*/
	@Override
	public String[] getDatastoreName() {
		return new String[]{"Mongo"};
	}




	

}
