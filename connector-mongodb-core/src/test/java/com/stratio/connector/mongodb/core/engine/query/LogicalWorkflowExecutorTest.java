/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.mongodb.core.engine.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.connector.mongodb.testutils.LogicalWorkFlowCreator;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ LogicalWorkflowExecutor.class, QueryResult.class, ResultSet.class })
public class LogicalWorkflowExecutorTest {

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    private static final ClusterName CLUSTER_NAME = new ClusterName("clustername");
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    /**
     * aggregationRequired() test
     * 
     * @throws Exception
     */
    @Test
    public void logicalWorkflowExecutorAggregationTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        LogicalWorkflowExecutor lwExecutor = new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));

        boolean aggregationRequired = (Boolean) Whitebox.getInternalState(lwExecutor, "aggregationRequired");

        assertFalse(aggregationRequired);
    }

    /**
     * readLogicalWorkflow() preparation test
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void logicalWorkflowExecutorPreparationTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addEqualFilter(COLUMN_1, 5, false, false);
        logWorkFlowCreator.addColumnName(COLUMN_1);
        logWorkFlowCreator.addColumnName(COLUMN_2);
        logWorkFlowCreator.addLimit(5);

        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        LogicalWorkflowExecutor lwExecutor = new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));

        Project project = (Project) Whitebox.getInternalState(lwExecutor, "projection");
        List<Filter> filterList = (List<Filter>) Whitebox.getInternalState(lwExecutor, "filterList");
        Limit limit = (Limit) Whitebox.getInternalState(lwExecutor, "limit");
        Select select = (Select) Whitebox.getInternalState(lwExecutor, "select");

        assertEquals(CATALOG, project.getCatalogName());
        assertEquals(1, filterList.size());
        assertEquals(Operator.EQ, ((Filter) filterList.get(0)).getRelation().getOperator());
        assertEquals(5, limit.getLimit());
        assertEquals(2, select.getColumnMap().size());

    }

    /**
     * readLogicalWorkflow() validation test
     * 
     * @throws Exception
     */
    @Test(expected = MongoValidationException.class)
    public void logicalWorkflowExecutorNoSelectTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        // removing the select. Adding a limit instead of the select
        for (LogicalStep logElement : logicalWorkflow.getLastStep().getPreviousSteps()) {
            logElement.setNextStep(new Limit(Operations.SELECT_LIMIT, 5));
        }

        new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));
    }

    @Test(expected = MongoValidationException.class)
    public void logicalWorkflowExecutorTwoProjectsTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        // removing the select. Adding a limit instead of the select
        for (LogicalStep logElement : logicalWorkflow.getLastStep().getPreviousSteps()) {
            logElement.setNextStep(new Project(Operations.PROJECT, new TableName(CATALOG, TABLE), CLUSTER_NAME));
        }

        new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));
    }

    @Test(expected = MongoValidationException.class)
    public void fulltextFilterTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addMatchFilter(COLUMN_1, "any");
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));
    }

    /**
     * buildQuery() test
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void logicalWorkflowExecutorPrepareQueryTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addEqualFilter(COLUMN_1, 5, false, false);
        logWorkFlowCreator.addGreaterFilter(COLUMN_2, 1, false);
        logWorkFlowCreator.addColumnName(COLUMN_1);
        logWorkFlowCreator.addColumnName(COLUMN_2);

        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();

        LogicalWorkflowExecutor lwExecutor = new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));

        List<DBObject> query = (List<DBObject>) Whitebox.getInternalState(lwExecutor, "query");

        assertEquals(1, query.size());

    }
}
