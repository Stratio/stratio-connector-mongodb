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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.mongodb.core.configuration.ConnectionConfigurationCreator;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.connector.mongodb.core.configuration.SupportedOperationsCreator;
import com.stratio.connector.mongodb.core.engine.MongoMetaProvider;
import com.stratio.connector.mongodb.core.engine.MongoMetadataEngine;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.security.ICredentials;



/**
 * This class implements the connector for Mongo. Created by darroyo on 8/07/14.
 */
public class MongoConnector implements IConnector {

	

	/**
	 * The Mongo client.
	 */
	private MongoClient mongoClient = null;

	/**
	 * The connector's configuration.
	 */
	private MongoClientConfiguration mongoConfiguration = null;
	
	/**
	 * The StorageEngine.
	 */
	private MongoStorageEngine mongoStorageEngine = null;

	/**
	 * The MetaProvider.
	 */
	private MongoMetaProvider mongoMetaProvider = null;

	/**
	 * The QueryEngine.
	 */
	private MongoQueryEngine mongoQueryEngine = null;
	
	private MongoMetadataEngine mongoMetadataEngine = null;
	
	/**
	* The Log.
	*/
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	

	/**
	 * Create a connection to Mongo.
	 * 
	 * @param credentials
	 *            the security credentials.
	 * @param configuration
	 *            the connection configuration.
	 *
	 * @throws InitializationException
	 *             on failure connection.
	 */
	
	@Override
	public void init(ICredentials credentials, IConfiguration configuration)
			throws InitializationException {
		
		//if not null=> return the previous connection
				
				if(!isConnected()){

					mongoConfiguration = new MongoClientConfiguration(configuration);
					
					ArrayList<ServerAddress> seeds = new ArrayList<ServerAddress>(3);
					
					for(String server: mongoConfiguration.getSeeds()){
						String[] array =  server.split(":");
						try{
							if(array.length != 2) throw new InitializationException("invalid address => host:port");
							else seeds.add(new ServerAddress(array[0],Integer.decode(array[1])));
							
						}catch(UnknownHostException e){
							throw new InitializationException("connection failed");
						}
					}
					
					createClient(seeds, credentials);

				}else {
					throw new InitializationException("connection already exist");
				}

				
		
	}
	
	

 /**
* Create the client.
*
* @param seeds the list of servers.
* @param credentials   the security credentials.
* @throws InitializationException in case the connection fail.
*/
    private void createClient(List<ServerAddress> seeds, ICredentials credentials) throws InitializationException {
        if (mongoClient == null ) {
		
				if (credentials == null) {
							mongoClient = new MongoClient(seeds,mongoConfiguration.getMongoClientOptions());//Excep
							logger.info("MongoDB connection established ");
					} else {
						
						throw new InitializationException("Credentials are not supported");
			
						// deprecated boolean auth = db.authenticate(myUserName,
						// myPassword);
			
			//			ArrayList<MongoCredential> credentialsSet = new ArrayList<MongoCredential>(1);
			//			credentialsSet.add(MongoCredential.createMongoCRCredential(
			//					credentials.getUser(), MongoConnector.database, credentials
			//							.getPass().toCharArray()));
			//
			//			try {
			//				mongoClient = new MongoClient(seeds, credentialsSet);
			//			} catch (MongoException | UnknownHostException e) {
			//				throw new ConnectionException("connection failed to [" + hostM
			//						+ ":" + portM + "]", e);
			//			}
			
					}
					
  
        }
    }	
	
	


	
	/**
	 * Close the Mongo's connection.
	 * 
	 */
	public void close() {

		if(mongoClient != null){
			mongoClient.close();
			logger.info("Disconnected from Mongo");
		}
		mongoClient = null;
		mongoStorageEngine = null;
		mongoQueryEngine = null;
		mongoMetaProvider = null;
	}

	/**
	 * Return the StorageEngine.
	 * 
	 * @return the StorageEngine
	 */
	@Override
	public IStorageEngine getStorageEngine() {
		createSingletonStorageEngine();
		mongoStorageEngine.setConnection(mongoClient);
		return mongoStorageEngine;

	}

	/**
	 * Return the MetadataProvider.
	 * 
	 * @return the MetadataProvider
	 */
	public IMetadataProvider getMedatadaProvider(){
		createSingletonMetaProvider();
		mongoMetaProvider.setConnection(mongoClient);
		return mongoMetaProvider;
	}

	/**
	 * Return the QueryEngine.
	 * 
	 * @return the QueryEngine
	 */
	@Override
	public IQueryEngine getQueryEngine(){
		createSingletonQueryEngine();
		mongoQueryEngine.setConnection(mongoClient);
		return mongoQueryEngine;
	}

//	@Override
//	public IConnectorConfiguration getConnectorConfiguration() {
//		return null;
//	}

	/**
	 * Return the configuration.
	 * 
	 * @return the configuration.
	 */
	public IConfiguration getConfiguration() {
		return mongoConfiguration;
	}

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
	
	
	/**
	 * Create a StorageEngine.
	 */
	private void createSingletonStorageEngine() {
		if (mongoStorageEngine == null) {
			mongoStorageEngine = new MongoStorageEngine();
		}
	}


	/**
	 * Create a QueryEngine.
	 */
	private void createSingletonQueryEngine() {
		if (mongoQueryEngine == null) {
			mongoQueryEngine = new MongoQueryEngine();
		}
	}

	/**
	 * Create a MetaProvider.
	 */
	private void createSingletonMetaProvider() {
		if (mongoMetaProvider == null) {
			mongoMetaProvider = new MongoMetaProvider();
		}
	}
	
	/**
	 * Create a MetadataEngine.
	 */
	private void createSingletonMetadataEngine() {
		if (mongoMetadataEngine == null) {
			mongoMetadataEngine = new MongoMetadataEngine();
		}
		
	}
	
	
	/**
	* Return the DataStore Name.
	* @return DataStore Name
	*/
	@Override
	public String getDatastoreName() {
		return "Mongo";
	}

	/**
	* The connection status.
	*
	* @return true if the driver's client is not null.
	*/
	@Override
	public boolean isConnected() {
		return mongoClient != null;
//		boolean connected=false;
//		ReplicaSetStatus rsStatus;
//		if ( mongoClient != null ) {
//			if( (rsStatus= mongoClient.getReplicaSetStatus()) == null) connected = true; //mongo getRSStatus?
//			else connected = (rsStatus.getMaster() != null); //TODO canRead(RP) and canWrite(WC) //puede ser proceso de failover
//		}
//		return connected;

	}



	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IConnector#getMetadataEngine()
	 */
	@Override
	public IMetadataEngine getMetadataEngine() throws UnsupportedException {
		createSingletonMetadataEngine();
		mongoMetadataEngine.setStorageEngine((MongoStorageEngine) getStorageEngine());
		mongoMetadataEngine.setQueryEngine((MongoQueryEngine) getQueryEngine());
		return mongoMetadataEngine;
	}

	

}
