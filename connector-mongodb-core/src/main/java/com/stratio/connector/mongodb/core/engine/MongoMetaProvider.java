package com.stratio.connector.mongodb.core.engine;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.meta.exception.UnsupportedOperationException;

/**
* This class represents a MetaInfo Provider for Mongo.
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
	public void createIndex(String catalog, String tableName, String... fields) throws UnsupportedOperationException {
		
		DBObject indexDBObject = new BasicDBObject();
		//indice text, hash =>1
		for(int i = 0; i< fields.length;i++){
			indexDBObject.put(fields[i], 1);
		}

		mongoClient.getDB(catalog).getCollection(tableName).
		createIndex(indexDBObject);
		
	}

	@Override
	public void dropIndex(String catalog, String tableName, String... fields)
			throws UnsupportedOperationException {
		
		DBObject indexDBObject = new BasicDBObject();

		for(int i = 0; i< fields.length;i++){
			indexDBObject.put(fields[i], 1);
		}
		mongoClient.getDB(catalog).getCollection(tableName).dropIndex(indexDBObject);
		
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
