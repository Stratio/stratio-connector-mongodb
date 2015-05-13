/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.mongodb.core.engine;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.SingleProjectQueryEngine;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutor;
import com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutorFactory;
import com.stratio.connector.mongodb.core.engine.query.MongoLogicalWorkflowValidator;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * The MongoQueryEngine.
 */
public class MongoQueryEngine extends SingleProjectQueryEngine<MongoClient> {


    private static MongoQueryEngine instance = null;
    /**
     * Instantiates a new Mongo query engine.
     *
     * @param connectionHandler
     *            the connection handler
     */
    private MongoQueryEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    public static MongoQueryEngine getInstance(MongoConnectionHandler connectionHandler){
        if(instance == null){
            instance = new MongoQueryEngine(connectionHandler);
        }
        return instance;
    }

    /**
     * Instantiates a new Mongo query engine.
     *
     * @param project
     *            the initial step of the logical workflow
     * @param connection
     *            the MongoDB connection
     * @return the query result
     * @throws MongoValidationException
     *             if the specified operation is not supported
     * @throws ConnectorException
     *             if the execution fails
     */
    @Override
    public QueryResult execute(Project project, Connection<MongoClient> connection) throws MongoValidationException,
                    ConnectorException {

        ProjectParsed logicalWorkfloParsed = new ProjectParsed(project, new MongoLogicalWorkflowValidator());
        LogicalWorkflowExecutor executor = LogicalWorkflowExecutorFactory
                        .getLogicalWorkflowExecutor(logicalWorkfloParsed);
        ResultSet resultSet = executor.executeQuery((MongoClient) connection.getNativeConnection());

        return QueryResult.createQueryResult(resultSet, 0, true);

    }

    @Override
    protected void asyncExecute(String s, Project project, Connection connection, IResultHandler iResultHandler) throws ConnectorException {
        throw new UnsupportedException("The method asyncExecute is not supported");
    }

    @Override
    protected void pagedExecute(String s, Project project, Connection connection, IResultHandler iResultHandler) throws ConnectorException {
        throw new UnsupportedException("The method pagedExecute is ot supported");

    }


    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IQueryEngine#stop(java.lang.String)
     */
    @Override
    public void stop(String queryId) throws UnsupportedException {
        throw new UnsupportedException("Not supported");

    }

}
