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
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.IConnector;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.InitializationException;
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
	 * The supported operations.
	 */
	private static final Map<Operations, Boolean> supportedOperations = null;
	
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
    private synchronized void createClient(List<ServerAddress> seeds, ICredentials credentials) throws InitializationException {
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
	 * Close the Aerospike's connection.
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
		createSingeltonStorageEngine();
		mongoStorageEngine.setConnection(mongoClient);
		return mongoStorageEngine;

	}

	/**
	 * Return the MetadataProvider.
	 * 
	 * @throws ConnectionException
	 *             in case that the connection is not initialized or is not
	 *             open.
	 * @return the MetadataProvider
	 *
	 */
	public IMetadataProvider getMedatadaProvider(){
		createSingeltonMetaProvider();
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
		createSingeltonQueryEngine();
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
    
    public Set<ConnectionConfiguration> getConnectionConfiguration(){
    	return ConnectionConfigurationCreator.getConfiguration();
    }
	
	
	/**
	 * Create a Singleton StorageEngine.
	 */
	private synchronized void createSingeltonStorageEngine() {
		if (mongoStorageEngine == null) {
			mongoStorageEngine = new MongoStorageEngine();
		}
	}


	/**
	 * Create a Singleton QueryEngine.
	 */
	private synchronized void createSingeltonQueryEngine() {
		if (mongoQueryEngine == null) {
			mongoQueryEngine = new MongoQueryEngine();
		}
	}

	/**
	 * Create a Singleton MetaProvider.
	 */
	private synchronized void createSingeltonMetaProvider() {
		if (mongoMetaProvider == null) {
			mongoMetaProvider = new MongoMetaProvider();
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

	@Override
	public boolean isConnected() {
		
		return mongoClient != null;
//		boolean connected=false;
//		if(mongoClient != null){
//			connected=true;
//			//mongoClient.getReplicaSetStatus().
//		}
//		return connected;
	}

	

}
