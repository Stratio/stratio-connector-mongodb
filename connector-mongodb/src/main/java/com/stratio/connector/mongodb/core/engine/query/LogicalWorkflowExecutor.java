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

package com.stratio.connector.mongodb.core.engine.query;

import java.util.List;

import com.stratio.connector.commons.TimerJ;
import com.stratio.connector.commons.connection.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.engine.query.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.GroupByDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.LimitDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.OrderByDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Prepares and performs MongoDB queries from a logical workflow.
 */
public abstract class LogicalWorkflowExecutor {

    /** The logger. */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /** The logical workflow data. */
    protected ProjectParsed logicalWorkflowData;

    /** The MongoDB query. */
    protected List<DBObject> query = null;

    /**
     * Instantiates a new logical workflow executor ready for the execution.
     *
     * @param logicalWorkflowParsed
     *            the logical workflow parsed.
     * @throws ExecutionException
     *             if the execution fails or the query specified in the logical workflow is not supported.
     * @throws UnsupportedException
     *             if the specified operation is not supported.
     */
    public LogicalWorkflowExecutor(ProjectParsed logicalWorkflowParsed) throws ExecutionException, UnsupportedException {
        logicalWorkflowData = logicalWorkflowParsed;
        this.buildQuery();
    }

    /**
     * Builds the query.
     *
     * @throws ExecutionException
     *             if the execution fails or the query specified in the logical workflow is not supported.
     * @throws UnsupportedException
     *             if the specified operation is not supported.
     */
    @TimerJ
    protected abstract void buildQuery() throws ExecutionException, UnsupportedException;

    /**
     * Execute the query.
     *
     * @param connection
     *            the Connection that holds the client.
     * @throws ExecutionException
     *             if the execution fails or the query specified in the logical workflow is not supported.
     * @return the Crossdata ResultSet.
     */
    public abstract ResultSet executeQuery(Connection<MongoClient> connection) throws ExecutionException;

    /**
     * Builds the group by.
     *
     * @return the DB object.
     * @throws ExecutionException
     *             if the execution fails.
     */
    protected DBObject buildGroupBy() throws ExecutionException {

        GroupByDBObjectBuilder groupDBObject = new GroupByDBObjectBuilder(logicalWorkflowData.getGroupBy(),
                        logicalWorkflowData.getSelect().getColumnMap().keySet());
        return groupDBObject.build();
    }

    /**
     * Builds the limit.
     *
     * @return the DB object.
     */
    protected DBObject buildLimit() {
        LimitDBObjectBuilder limitDBObject = new LimitDBObjectBuilder(logicalWorkflowData.getLimit());
        return limitDBObject.build();
    }

    /**
     * Builds the project.
     *
     * @param aggregationRequired
     *            whether the query use the aggregation framework or not.
     * @return the DB object.
     * @throws ExecutionException
     *             if the project specified in the logical workflow is not supported
     */
    protected DBObject buildProject(boolean aggregationRequired) throws ExecutionException {
        ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(aggregationRequired,
                        logicalWorkflowData.getProject(), logicalWorkflowData.getSelect());
        return projectDBObject.build();
    }

    /**
     * Builds the filter.
     *
     * @param aggregationRequired
     *            whether the query use the aggregation framework or not.
     * @return the DB object.
     * @throws MongoValidationException
     *             if any filters specified in the logical workflow is not supported.
     * @throws UnsupportedException
     *             if the specified filter operation is not supported
     */
    protected DBObject buildFilter(boolean aggregationRequired) throws MongoValidationException, UnsupportedException {

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(aggregationRequired,
                        logicalWorkflowData.getFilter());
        return filterDBObjectBuilder.build();
    }

    /**
     * Builds the orderBy.
     *
     * @param aggregationRequired
     *            whether the query use the aggregation framework or not.
     * @return the DB object.
     * @throws ExecutionException
     *             if the execution fails.
     */
    protected DBObject buildOrderBy(boolean aggregationRequired) throws ExecutionException {

        OrderByDBObjectBuilder orderByDBObjectBuilder = new OrderByDBObjectBuilder(aggregationRequired,
                        logicalWorkflowData.getOrderBy());
        return orderByDBObjectBuilder.build();
    }

}