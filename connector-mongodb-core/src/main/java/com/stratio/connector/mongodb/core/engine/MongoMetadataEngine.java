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


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * @author darroyo
 *
 */
public class MongoMetadataEngine implements IMetadataEngine{

	private transient MongoConnectionHandler connectionHandler;
	
	

	/**
	 * @param connectionHandler
	 */
	public MongoMetadataEngine(MongoConnectionHandler connectionHandler) {
		this.connectionHandler = connectionHandler;
	}

	 @Override
	 public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws UnsupportedException {
		 throw new UnsupportedException("Not yet supported");
	 }
	 @Override
	 public void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException {
		 throw new UnsupportedException("Not yet supported");
	 }
	 @Override
	 public void dropCatalog(ClusterName targetCluster, CatalogName name) throws ExecutionException {
		 try {
			recoveredClient(targetCluster).dropDatabase(name.getName());
		} catch (HandlerConnectionException e) {
			throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
		}
	 }
	 @Override
	 public void dropTable(ClusterName targetCluster, TableName name) throws ExecutionException {
		 
		 DB db;
		try {
			db = recoveredClient(targetCluster).getDB(name.getCatalogName().getName());
		} catch (HandlerConnectionException e) {
			throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
		}
		db.getCollection(name.getName()).drop();
    	 
	 }
		
/*
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
			
		//TODO TextIndexes??
	 
	*/ 
	 
	 
	 
	 
	 
	 
	 private MongoClient recoveredClient(ClusterName targetCluster) throws HandlerConnectionException {
		 return (MongoClient) connectionHandler.getConnection(targetCluster.getName()).getNativeConnection();
	 }

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#createIndex(com.stratio.meta2.common.data.ClusterName, com.stratio.meta2.common.metadata.IndexMetadata)
	 */
	@Override
	public void createIndex(ClusterName targetCluster,IndexMetadata indexMetadata) throws UnsupportedException,
			ExecutionException {
		 
		 DB db;
		try {
			db = recoveredClient(targetCluster).getDB(indexMetadata.getName().getTableName().getCatalogName().getName());
		} catch (HandlerConnectionException e) {
			throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
		}
		
		DBObject indexDBObject = new BasicDBObject();
		
		if(indexMetadata.getType() == IndexType.DEFAULT){
		
			for (ColumnMetadata columnMeta : indexMetadata.getColumns()){
				indexDBObject.put(columnMeta.getName().getName(), 1);
				//TODO compound index with default order (1,-1), list of columnMeta are supposed to be ordered
				//TODO getIndex()? columnMetada solo incluye las columnas a indexar?? o hay que comprobar
			}
			
		}else if(indexMetadata.getType() == IndexType.FULL_TEXT){
			for (ColumnMetadata columnMeta : indexMetadata.getColumns()){
				indexDBObject.put(columnMeta.getName().getName(), "text");
				//TODO above, options, compounds
			}

			//TODO hash index for shards??
		}else throw new UnsupportedException("index type "+indexMetadata.getType().toString()+" is not supported");

		db.getCollection(indexMetadata.getName().getTableName().getName()).createIndex(indexDBObject);
		
	}

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#dropIndex(com.stratio.meta2.common.data.ClusterName, com.stratio.meta2.common.metadata.IndexMetadata)
	 */
	@Override
	public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
			throws UnsupportedException, ExecutionException {
		 DB db;
			try {
				db = recoveredClient(targetCluster).getDB(indexMetadata.getName().getTableName().getCatalogName().getName());
			} catch (HandlerConnectionException e) {
				throw new ExecutionException("cluster cannot be recovered: "+targetCluster.getName() , e);
			}
			
			DBObject indexDBObject = new BasicDBObject();
			
			if(indexMetadata.getType() == IndexType.DEFAULT){
			
				for (ColumnMetadata columnMeta : indexMetadata.getColumns()){
					indexDBObject.put(columnMeta.getName().getName(), 1);
					//TODO compound index with default order (1,-1), list of columnMeta are supposed to be ordered
					//TODO getIndex()? columnMetada solo incluye las columnas a indexar?? o hay que comprobar
				}
				db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(indexDBObject);
				//TODO check the operation code
			}else if(indexMetadata.getType() == IndexType.FULL_TEXT){
				String defaultTextIndexName = "";
				
				
				int colNumber = 0;
				int columnSize = indexMetadata.getColumns().size();
				
				for (ColumnMetadata columnMeta : indexMetadata.getColumns()){
					defaultTextIndexName+= columnMeta.getName().getName()+"_";
					indexDBObject.put(columnMeta.getName().getName(), "text");
					
					if(++colNumber != columnSize) defaultTextIndexName+="_";
					//TODO above, options, compounds. http://docs.mongodb.org/manual/tutorial/avoid-text-index-name-limit/
					
				}
				//TODO hash index for shards??
			}else throw new UnsupportedException("index type "+indexMetadata.getType().toString()+" is not supported");


			
	}

	
//	/* (non-Javadoc)
//	 * @see com.stratio.meta.common.connector.IMetadataEngine#put(java.lang.String, java.lang.String)
//	 */
//	@Override
//	public void put(String key, String metadata) {
//		
//		if(key.contains(".") || metadata.contains(".")) {
//			//TODO throw new ExecutionException("The character '.' is not allowed"){};
//			System.out.println("add ExecutionException . not allowed");
//		}else{
//			//TODO validate? key exist? =>key=type y id=1
//			//get then insert?
//			
//			Row row = new Row();
//	        Map<String, Cell> cells = new HashMap<>();
//	        cells.put(key, new Cell(metadata));
//	        row.setCells(cells);
//			storageEngine.insert(CATALOG, COLLECTION, row, key);
//		}
//		
//	}
//
//	/* (non-Javadoc)
//	 * @see com.stratio.meta.common.connector.IMetadataEngine#get(java.lang.String)
//	 */
//	@Override
//	public String get(String key) {
//		Object value;
//		Cell cell;
//		if ((cell= queryEngine.getRowById(CATALOG, COLLECTION, key).getCell(key)) != null){
//			if ((value =cell.getValue()) != null){
//				return (value instanceof String) ? (String) value : null;
//			} else return null;
//		}else return null;
//        
//	}


}
