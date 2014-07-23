package com.stratio.connector.mongodb.core.engine;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.meta.exception.UnsupportedOperationException;

/**
* This class represents a MetaInfo Provider for Mongo.
* Created by darroyo on 10/07/14.
*/
public class MongoMetaProvider implements IMetadataProvider {
	 /**
	* The connection.
	*/
	private MongoClient mongoClient = null;
	

    @Override
    public void createCatalog(String catalog) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void createTable(String catalog, String table) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void dropCatalog(String catalog) throws UnsupportedOperationException {
        mongoClient.dropDatabase(catalog);
    }

 
    @Override
    public void dropTable(String catalog, String table) throws UnsupportedOperationException {
    	 DB db = mongoClient.getDB(catalog);
    	 db.getCollection(table).drop();
    	
    }
	

	@Override
	public void createIndex(String catalog, String tableName, String field) throws UnsupportedOperationException {
		DBObject keys = new BasicDBObject();
		keys.put(field, 1);
		mongoClient.getDB(catalog).getCollection(tableName).createIndex(keys);
		
	}

	@Override
	public void dropIndex(String catalog, String tableName, String field)
			throws UnsupportedOperationException {
		mongoClient.getDB(catalog).getCollection(tableName).dropIndex(new BasicDBObject(field, 1));
		
	}

	@Override
	public void dropIndexes(String catalog, String tableName)
			throws UnsupportedOperationException {
		mongoClient.getDB(catalog).getCollection(tableName).dropIndexes();
		
	}
		
	//TextIndexes??
		
   /**
	* Set the connection.
	* @param aerospikeClient the connection.
	*/
    public void setConnection(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }
		
	
}
