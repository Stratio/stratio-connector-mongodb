package com.stratio.connector.mongodb.core.engine;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.bson.types.Binary;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Filter;



/**
 * This class performs operations insert and delete in Mongo.
 * Created by darroyo on 10/07/14.
 */
public class MongoStorageEngine implements IStorageEngine {

	private MongoClient mongoClient = null;
	
	/**
     * Insert a document in MongoDB.
     *
     * @param catalog		the database.
     * @param tableName     the collection.
     * @param row      		the row.
     * @throws ExecutionException  in case of failure during the execution.
     */
	public void insert(String catalog, String tableName, Row row)
			throws UnsupportedOperationException {

		if (isEmpty(catalog) || isEmpty(tableName) || row == null) {
			//throw exception
		}else{
			//try..catch
				
				//List<String> dbNames = mongoClient.getDatabaseNames();
				// do while => check
			
				DB db = mongoClient.getDB(catalog);
				BasicDBObject doc = new BasicDBObject();
				// doc.putAll(row.getCells());//si cambio de Row...
				for (Map.Entry<String, Cell> entry : row.getCells().entrySet())
				{
					Object cellValue = entry.getValue().getValue();	
					doc.put(entry.getKey(), cellValue);
				}	
//				CHECK BEFORE INSERT?			
//					if(cellValue instanceof Integer || 
//							cellValue instanceof Pattern || 
//							...
//							cellValue instanceof Date || 
//							cellValue instanceof DBRef || 
//							cellValue instanceof Binary || 
//							cellValue instanceof byte[] || //check 
//							cellValue instanceof Boolean || 
//							
//							){
//					}

				db.getCollection(tableName).insert(doc);// no options(alt create)
				// insert(doc,write concern)			
			
		}
			
	}

	/**
     * Insert a set of documents in MongoDB.
     *
     * @param catalog		the database.
     * @param tableName     the collection.
     * @param row      		the row.
     * @throws ExecutionException  in case of failure during the execution.
     */
	public void insert(String catalog, String tableName, Set<Row> rows)
			throws UnsupportedOperationException {
	
		for(Row row: rows){
			insert(catalog, tableName, row);
		}
		
		
		
//		_id required	
//		if (isEmpty(catalog) || isEmpty(tableName) || rows == null || rows.isEmpty()) {
//			//throwException
//		}else{
//		
//
//			
//			DB db = mongoClient.getDB(catalog);
//			BulkWriteOperation bulk = db.getCollection(tableName).initializeUnorderedBulkOperation();
//			
//			
//			BasicDBObject doc = new BasicDBObject();
//			for (Row row: rows){
//				for (Map.Entry<String, Cell> entry : row.getCells().entrySet())
//				{
//					Object cellValue = entry.getValue().getValue();
//					doc.put(entry.getKey(), cellValue);  
//				}
//				bulk.insert(doc);
//			}
//			bulk.execute();
//		}
		
						
	}


	public void delete(String catalog, String tableName, Filter filter)
			throws UnsupportedOperationException {
		//TODO list Filter.  And, Or, etc...
		
		DB db = mongoClient.getDB(catalog);

		if (db.collectionExists(tableName)) {
			DBCollection coll = db.getCollection(tableName);
			FilterDBObject fil = new FilterDBObject();
			fil.addFilter(filter);
			DBObject query = fil.getFilterQuery();
			coll.remove(query);	
		}
		

	}

//	UPDATE??
//	http://docs.mongodb.org/manual/reference/operator/update/
	
	private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
	
	/**
	 * Set the connection.
	 * 
	 * @param mongoClient
	 *            the connection.
	 */
	public void setConnection(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}
}
