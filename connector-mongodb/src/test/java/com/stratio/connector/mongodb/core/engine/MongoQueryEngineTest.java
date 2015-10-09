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

import com.stratio.crossdata.common.exceptions.ConnectorException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutor;
import com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutorFactory;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

@PowerMockIgnore( {"javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ MongoQueryEngine.class, LogicalWorkflowExecutor.class, LogicalWorkflowExecutorFactory.class,
        QueryResult.class, ResultSet.class })
public class MongoQueryEngineTest {

    private MongoQueryEngine mongoQueryEngine;
    @Mock
    MongoConnectionHandler connectionHandler;

    @Before
    public void before() throws Exception {

        mongoQueryEngine = MongoQueryEngine.getInstance(connectionHandler);

    }

    /**
     * Method: execute(Project logicalWorkflow, Connection<Client> connection)
     */
    @SuppressWarnings("unchecked")
    @Test
    public void executeTest() throws Exception {
        Project project = mock(Project.class);
        LogicalWorkflowExecutor logicalWorkflowExecutor = mock(LogicalWorkflowExecutor.class);
        Connection<MongoClient> connection = mock(Connection.class);

        ProjectParsed projectParsed = mock(ProjectParsed.class);
        PowerMockito.whenNew(ProjectParsed.class).withAnyArguments().thenReturn(projectParsed);
        PowerMockito.mockStatic(LogicalWorkflowExecutorFactory.class);
        PowerMockito.when(LogicalWorkflowExecutorFactory.getLogicalWorkflowExecutor(Matchers.any(ProjectParsed.class)))
                .thenReturn(logicalWorkflowExecutor);
        ResultSet resultSet = mock(ResultSet.class);
        when(logicalWorkflowExecutor.executeQuery(connection)).thenReturn(resultSet);
        QueryResult queryResult = mock(QueryResult.class);
        PowerMockito.mockStatic(QueryResult.class);
        PowerMockito.when(QueryResult.createQueryResult(resultSet, 0, true)).thenReturn(queryResult);

        QueryResult returnQueryResult = mongoQueryEngine.execute(project, connection);

        verify(logicalWorkflowExecutor, times(1)).executeQuery(connection);

        PowerMockito.verifyStatic(times(1));
        QueryResult.createQueryResult(resultSet, 0, true);

        assertEquals("The query result is wrong", queryResult, returnQueryResult);
    }

    /**
     * Method: asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
     */
    @Test(expected = UnsupportedException.class)
    public void asyncExecuteTest() throws ConnectorException {

        mongoQueryEngine.asyncExecute("", Mockito.mock(Project.class), null,Mockito.mock(IResultHandler.class));

    }

    /**
     * Method: stop(String queryId)
     */
    @Test(expected = UnsupportedException.class)
    public void stopTest() throws UnsupportedException, ExecutionException {
        mongoQueryEngine.stop("");
    }

}
