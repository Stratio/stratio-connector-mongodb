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

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;




public class MongoQueryEngine implements IQueryEngine {

	private transient MongoConnectionHandler connectionHandler;

    /**
	 * @param connectionHandler
	 */
	public MongoQueryEngine(MongoConnectionHandler connectionHandler) {
		this.connectionHandler = connectionHandler;
	}

	@Override
	public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws ExecutionException, UnsupportedException {
    	
    	MongoResultSet resultSet =null;
    	LogicalWorkflowExecutor executor = new LogicalWorkflowExecutor(workflow);
    	try {
			resultSet = executor.executeQuery(recoveredClient(targetCluster));
		} catch (HandlerConnectionException e) {
			throw new ExecutionException("client cannot have been recovered",e);
		}  		
    	return QueryResult.createQueryResult(resultSet); 	

    }
	 
	 private MongoClient recoveredClient(ClusterName targetCluster) throws HandlerConnectionException {
		 return (MongoClient) connectionHandler.getConnection(targetCluster.getName()).getNativeConnection();
	 }
	 
//	protected Row getRowById(ClusterName targetCluster, String catalog, String collection, String id) throws ExecutionException{
//		MongoClient mongoClient = null;
//		try {
//			mongoClient = recoveredClient(targetCluster);
//		} catch (HandlerConnectionException e) {
//			throw new ExecutionException("client cannot have been recovered",e);
//		} 
//		
//		DBCursor cursor = mongoClient.getDB(catalog).getCollection(collection).find(new BasicDBObject("_id", id));
//	    	
//	    Row row = new Row();
//	    	
//	    while (cursor.hasNext()){
//	    	DBObject rowDBObject = cursor.next();
//	    	for (String field : rowDBObject.keySet()) {
//	    		row.addCell(field, new Cell(rowDBObject.get(field)));
//	    	}
//	    }
//		
//	    return row;
//	    	
//		}
}
