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
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.UniqueProjectQueryEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.connector.IResultHandler;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;

public class MongoQueryEngine extends UniqueProjectQueryEngine<MongoClient> {

    /**
     * @param connectionHandler
     */
    public MongoQueryEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    public QueryResult execute(Project workflow, Connection<MongoClient> connection) throws ExecutionException,
                    UnsupportedException {

        ResultSet resultSet = null;

        LogicalWorkflowExecutor executor = new LogicalWorkflowExecutor(workflow);

        resultSet = executor.executeQuery((MongoClient) connection.getNativeConnection());

        return QueryResult.createQueryResult(resultSet);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#asyncExecute(java.lang.String,
     * com.stratio.meta.common.logicalplan.LogicalWorkflow, com.stratio.meta.common.connector.IResultHandler)
     */
    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
                    throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#stop(java.lang.String)
     */
    @Override
    public void stop(String queryId) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");

    }

}
