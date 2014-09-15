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
package com.stratio.connector.mongodb.core.engine;


import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoDeleteException;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.TableMetadata;



/**
 * This class performs operations insert and delete in Mongo.
 * Created by darroyo on 10/07/14.
 */
public class MongoStorageEngine implements IStorageEngine {

	private transient MongoConnectionHandler connectionHandler;
	
	/**
	 * @param connectionHandler
	 */
	public MongoStorageEngine(MongoConnectionHandler connectionHandler) {
		this.connectionHandler = connectionHandler;
	}

	
	 @Override
	 public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException, ExecutionException {
		 try {
			insert((MongoClient)connectionHandler.getConnection(targetCluster.getName()).getNativeConnection(), targetTable, row);
		} catch (HandlerConnectionException e) {
			throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
		}
	 }
	 
	 
	 @Override
	 public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows ) throws UnsupportedException, ExecutionException {
		 try{
			 insert((MongoClient)connectionHandler.getConnection(targetCluster.getName()).getNativeConnection(), targetTable, rows);
		 } catch (HandlerConnectionException e) {
				throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
			}
	 }
	 
	 
	/**
     * Insert a document in MongoDB.
     *
     * @param catalog		the database.
     * @param tableName     the collection.
     * @param row      		the row.
     * @throws ExecutionException  in case of failure during the execution.
     */
	private void insert(MongoClient mongoClient, TableMetadata targetTable, Row row) throws ExecutionException, UnsupportedException {

		String catalog = targetTable.getName().getCatalogName().getName();
		String tableName = targetTable.getName().getName();
		
		if (isEmpty(catalog) || isEmpty(tableName) || row == null) {
			//throw exception
		}else{
			//try..catch
				
				//List<String> dbNames = mongoClient.getDatabaseNames();
				// do while => check
			
				DB db = mongoClient.getDB(catalog);
				BasicDBObject doc = new BasicDBObject();
				
				String pk = null;
			
				for (Map.Entry<String, Cell> entry : row.getCells().entrySet())
				{
					Object cellValue = entry.getValue().getValue();	
					doc.put(entry.getKey(), cellValue);
					
					if (targetTable.isPK(new ColumnName(targetTable.getName().getCatalogName().getName(),targetTable.getName().getName(), entry.getKey()))){
						if (pk!=null) throw new UnsupportedException("Only one PK is allowed");
						pk = entry.getValue().getValue().toString(); //TODO revisar el toString.
					}
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
				if (pk!=null){
					//TODO upsert by default
					BasicDBObject find = new BasicDBObject();
					find.put("_id", pk);
					
					db.getCollection(tableName).update(find,new BasicDBObject("$set", doc),true,false); 
					
				}else {
					db.getCollection(tableName).insert(doc);// no options(alt create)
					// insert(doc,write concern)			
				}
				
				
			
		}
			
	}
	
//	UPDATE??
//	http://docs.mongodb.org/manual/reference/operator/update/

	/**
     * Insert a set of documents in MongoDB.
     *
     * @param catalog		the database.
     * @param tableName     the collection.
     * @param row      		the row.
     * @throws ExecutionException  in case of failure during the execution.
     */
	private void insert(MongoClient mongoClient, TableMetadata targetTable, Collection<Row> rows)
			throws UnsupportedException, ExecutionException {		
		
		for(Row row: rows){
			insert(mongoClient, targetTable, row);
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

	/* Delete a set of documents.
	*
	* @param catalog the catalog.
	* @param tableName   the collection.
	* @param filterSet filters to restrict the set of documents.
	*/
	private void delete(MongoClient mongoClient, String catalog, String tableName, Filter... filterSet)
			throws UnsupportedOperationException {
		//TODO list Filter.  And, Or, etc...

		
		DB db = mongoClient.getDB(catalog);

		if (db.collectionExists(tableName)) {
			DBCollection coll = db.getCollection(tableName);
			FilterDBObjectBuilder filterBuilder = new FilterDBObjectBuilder(false);
			
			for(Filter filter: filterSet){
				filterBuilder.add(filter);
			}

			coll.remove(filterBuilder.build());	
		}
		

	}


	
	private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
	
	 
	
	
	 private MongoClient recoveredClient(ClusterName targetCluster) throws HandlerConnectionException {
		 return (MongoClient) connectionHandler.getConnection(targetCluster.getName()).getNativeConnection();
	 }
	
}
