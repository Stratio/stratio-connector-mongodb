package com.stratio.connector.mongodb.ftest;

import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.stratio.connector.meta.ConfigurationImplem;
import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.connector.mongodb.core.configuration.MongoClientConfiguration;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;

public class ConnectionTest {

    protected final String COLLECTION = getClass().getSimpleName();
    protected final String CATALOG = "functionaltest";
    /**
     * The aerospike aerospikeClient.
     */
    protected MongoClient mongoClient = null;
    protected MongoConnector stratioMongoConnector = null;
    MongoClientOptions cilentOptions = null;

    @Before
    public void setUp() {
        try {
           
            stratioMongoConnector = new MongoConnector();
            stratioMongoConnector.init(null, new ConfigurationImplem());
            MongoClientConfiguration confi = (MongoClientConfiguration) stratioMongoConnector.getConfiguration();
            MongoClientOptions options =  confi.getMongoClientOptions();
            
            ArrayList<ServerAddress> seeds = new ArrayList<ServerAddress>(3);;
            
            for(String server: confi.getSeeds()){
				String[] array =  server.split(":");
				try{
					if(array.length != 2) throw new InitializationException("invalid address => host:port");
					else seeds.add(new ServerAddress(array[0],Integer.decode(array[1])));
					
				}catch(UnknownHostException e){
					e.printStackTrace();
					throw new InitializationException("Connection failed");
				}
			}
            
            mongoClient = new MongoClient(seeds);
            
            deleteSet();

        } catch (MongoException | InitializationException e) {
            e.printStackTrace();
        }
    }

    private void deleteSet() throws MongoException {
        mongoClient.dropDatabase(CATALOG);
        
    }

    @After
    public void tearDown() throws ConnectionException {
        mongoClient.close();
        stratioMongoConnector.close();
    }

}
