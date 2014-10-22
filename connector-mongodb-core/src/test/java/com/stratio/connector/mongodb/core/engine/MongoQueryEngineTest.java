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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutor;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ MongoQueryEngine.class, LogicalWorkflowExecutor.class, QueryResult.class, ResultSet.class })
public class MongoQueryEngineTest {

    private MongoQueryEngine mongoQueryEngine;
    @Mock
    MongoConnectionHandler connectionHandler;

    @Before
    public void before() throws Exception {
        mongoQueryEngine = new MongoQueryEngine(connectionHandler);
    }

    /**
     * Method: execute(Project logicalWorkflow, Connection<Client> connection)
     */
    @Test
    public void executeTest() throws Exception {
        Project project = mock(Project.class);
        LogicalWorkflowExecutor logicalWorkflowExecutor = mock(LogicalWorkflowExecutor.class);
        Connection<MongoClient> connection = mock(Connection.class);
        MongoClient mongoClient = mock(MongoClient.class);
        when(connection.getNativeConnection()).thenReturn(mongoClient);
        PowerMockito.whenNew(LogicalWorkflowExecutor.class).withArguments(project).thenReturn(logicalWorkflowExecutor);
        ResultSet resultSet = mock(ResultSet.class);
        when(logicalWorkflowExecutor.executeQuery(mongoClient)).thenReturn(resultSet);
        QueryResult queryResult = mock(QueryResult.class);
        PowerMockito.mockStatic(QueryResult.class);
        PowerMockito.when(QueryResult.createQueryResult(resultSet)).thenReturn(queryResult);

        QueryResult returnQueryResult = mongoQueryEngine.execute(project, connection);

        verify(logicalWorkflowExecutor, times(1)).executeQuery(mongoClient);

        PowerMockito.verifyStatic(times(1));
        QueryResult.createQueryResult(resultSet);

        assertEquals("The query result is correct", queryResult, returnQueryResult);
    }

    /**
     * Method: asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
     */
    @Test(expected = UnsupportedException.class)
    public void asyncExecuteTest() throws UnsupportedException, ExecutionException {
        mongoQueryEngine.asyncExecute("", Mockito.mock(LogicalWorkflow.class), Mockito.mock(IResultHandler.class));
    }

    /**
     * Method: stop(String queryId)
     */
    @Test(expected = UnsupportedException.class)
    public void stopTest() throws UnsupportedException, ExecutionException {
        mongoQueryEngine.stop("");
    }

}
