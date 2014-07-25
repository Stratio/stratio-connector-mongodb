package com.stratio.connector.mongodb.core.engine;

import com.mongodb.MongoClient;
import com.stratio.connector.meta.ICallBack;
import com.stratio.connector.meta.IResultSet;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.LogicalPlan;




public class MongoQueryEngine implements IQueryEngine {

    private MongoClient mongoClient = null;

    public IResultSet execute(LogicalPlan logicalPlan) throws UnsupportedOperationException, ExecutionException {
    	
    	MongoResultSet resultSet =null;
    	LogicalStepDecider decider = new LogicalStepDecider(logicalPlan);
    	resultSet = decider.executeQuery(mongoClient);  		
    	return resultSet; 	

    }

    public IResultSet execute(IResultSet previousResult, LogicalPlan logicalPlan) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    public IResultSet execute(LogicalPlan logicalPlan, ICallBack callback) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    

	
	  /**
* Set the connection.
* @param aerospikeClient the connection.
*/
    public void setConnection(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }
}
