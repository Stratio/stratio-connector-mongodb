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
