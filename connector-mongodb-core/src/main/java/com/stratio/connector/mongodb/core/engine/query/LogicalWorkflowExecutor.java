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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.mongodb.core.engine.query.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.GroupByDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.LimitDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.MetaResultUtils;
import com.stratio.connector.mongodb.core.engine.query.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.Operator;

/**
 * Prepares and performs MongoDB queries from a logical workflow.
 */
public class LogicalWorkflowExecutor {

    /** The logger. */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /** The projection. */
    private Project projection = null;

    /** The list of filters without including full-text filters. */
    private List<Filter> filterList = new ArrayList<Filter>();

    /** The limit. */
    private Limit limit = null;

    /** The select. */
    private Select select = null;

    /** The group by. */
    private GroupBy groupBy = null;

    /** Whether the aggregation framework is compulsory for logicalworkflow or not. */
    private boolean aggregationRequired;

    /** The query. */
    private List<DBObject> query = null;

    /**
     * Instantiates a new logical workflow executor ready for the execution.
     *
     * @param initialProject
     *            the initial project
     * @throws UnsupportedException
     *             if the query specified in the logical workflow is not supported
     * @throws ExecutionException
     */
    public LogicalWorkflowExecutor(LogicalStep initialProject) throws UnsupportedException, ExecutionException {

        readLogicalWorkflow(initialProject);
        aggregationRequired();
        buildQuery();

    }

    /**
     * Computes if the aggregation is required.
     */
    private void aggregationRequired() {
        // Aggregation features will be included in the next release
        // TODO if complex or sharded environment=>map-reduce or aggregation framework
        aggregationRequired = (groupBy != null);

    }

    /**
     * Validates the logical workflow and stores the needed steps.
     *
     * @param initialProject
     *            the initial project
     * @throws UnsupportedException
     *             the unsupported exception
     */
    private void readLogicalWorkflow(LogicalStep initialProject) throws UnsupportedException {
        LogicalStep logicalStep = initialProject;

        do {
            if (select != null) {
                throw new MongoValidationException("Select must be the last step");
            }

            if (logicalStep instanceof Project) {
                if (projection == null) {
                    projection = (Project) logicalStep;
                } else {
                    throw new MongoValidationException(" # Project > 1");
                }
            } else if (logicalStep instanceof Filter) {
                Filter step = (Filter) logicalStep;
                if (Operator.MATCH == step.getRelation().getOperator()) {
                    throw new MongoValidationException("Full-text queries not yet supported");
                } else {
                    filterList.add(step);
                }
            } else if (logicalStep instanceof Limit) {
                if (limit == null) {
                    limit = (Limit) logicalStep;
                } else {
                    throw new MongoValidationException(" # Limit > 1");
                }
            } else if (logicalStep instanceof GroupBy) {
                if (groupBy == null) {
                    groupBy = (GroupBy) logicalStep;
                } else {
                    throw new MongoValidationException(" # GroupBy > 1");
                }
            } else if (logicalStep instanceof Select) {
                select = (Select) logicalStep;
            } else {
                throw new MongoValidationException("Step unsupported" + logicalStep.getClass());
            }
        } while ((logicalStep = logicalStep.getNextStep()) != null);

        if (projection == null) {
            throw new MongoValidationException("Projection has not been found in the logical workflow");
        }
        if (select == null) {
            throw new MongoValidationException("Select has not been found in the logical workflow");
        }

    }

    /**
     * Checks if the aggregation is required.
     *
     * @return true, if the aggregation is required
     */
    private boolean isAggregationRequired() {
        return aggregationRequired;
    }

    /**
     * Creates the corresponding MongoDB query.
     *
     * @throws MongoValidationException
     *             if the query specified in the logical workflow is not supported
     * @throws ExecutionException
     */
    private void buildQuery() throws MongoValidationException, ExecutionException {
        query = new ArrayList<DBObject>();

        if (isAggregationRequired()) {
            // TODO It will be included in the following release
            if (!filterList.isEmpty()) {
                query.add(buildFilter());
            }
            if (groupBy != null) {
                query.add(buildGroupBy());
            }
            if (limit != null) {
                query.add(buildLimit());
            }
        } else {
            query.add(buildFilter());
        }

    }

    private DBObject buildGroupBy() throws ExecutionException {

        GroupByDBObjectBuilder groupDBObject = new GroupByDBObjectBuilder(groupBy, select.getColumnMap().keySet());
        return groupDBObject.build();
    }

    /**
     * Builds the limit.
     *
     * @return the DB object
     */
    private DBObject buildLimit() {
        LimitDBObjectBuilder limitDBObject = new LimitDBObjectBuilder(limit);
        return limitDBObject.build();
    }

    /**
     * Builds the project.
     *
     * @return the DB object
     * @throws MongoValidationException
     *             if the project specified in the logical workflow is not supported
     */
    private DBObject buildProject() throws MongoValidationException {
        ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(aggregationRequired, select);
        return projectDBObject.build();
    }

    /**
     * Builds the filter.
     *
     * @return the DB object
     * @throws MongoValidationException
     *             if any filters specified in the logical workflow is not supported
     */
    private DBObject buildFilter() throws MongoValidationException {

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(aggregationRequired, filterList);

        if (logger.isDebugEnabled()) {
            logger.debug("Filter:" + filterDBObjectBuilder.build());
        }
        return filterDBObjectBuilder.build();
    }

    /**
     * Execute the query.
     *
     * @param mongoClient
     *            the mongo client
     * @return the result set
     * @throws MongoQueryException
     *             if an error exist during the execution
     * @throws MongoValidationException
     *             if the query specified in the logical workflow is not supported
     */
    public ResultSet executeQuery(MongoClient mongoClient) throws MongoQueryException, MongoValidationException {

        DB db = mongoClient.getDB(projection.getCatalogName());
        DBCollection collection = db.getCollection(projection.getTableName().getName());
        ResultSet resultSet;

        if (isAggregationRequired()) {
            resultSet = executeAggregationQuery(collection);
        } else {
            resultSet = executeBasicQuery(collection);
        }

        resultSet.setColumnMetadata(MetaResultUtils.createMetadata(projection, select));

        return resultSet;
    }

    /**
     * Execute an usual query.
     *
     * @param collection
     *            the collection
     * @return the result set
     * @throws MongoQueryException
     *             if an error exist during the execution
     * @throws MongoValidationException
     *             if the query specified in the logical workflow is not supported
     */
    private ResultSet executeBasicQuery(DBCollection collection) throws MongoQueryException, MongoValidationException {

        ResultSet resultSet = new ResultSet();
        DBCursor cursor = collection.find(query.get(0), buildProject());
        if (limit != null) {
            cursor = cursor.limit(limit.getLimit());
        }
        DBObject rowDBObject;
        try {
            while (cursor.hasNext()) {

                rowDBObject = cursor.next();
                if (logger.isDebugEnabled()) {
                    logger.debug("SResult: " + rowDBObject);
                }
                resultSet.add(MetaResultUtils.createRowWithAlias(rowDBObject, select));
            }
        } catch (MongoException e) {
            throw new MongoQueryException(e.getMessage(), e);
        } finally {
            cursor.close();
        }
        return resultSet;
    }

    /**
     * Execute an aggregation query.
     *
     * @param collection
     *            the collection
     * @return the result set
     * @throws MongoQueryException
     *             if an error exist during the execution
     */
    private ResultSet executeAggregationQuery(DBCollection collection) throws MongoQueryException {
        ResultSet resultSet = new ResultSet();
        // AggregationOptions aggOptions = AggregationOptions.builder()
        // .allowDiskUse(true)
        // .batchSize(size)
        // pipeline,aggOptions => dbcursor
        try {
            int stage = 1;
            for (DBObject aggregationStage : query) {
                logger.debug("Aggregate framework stage (" + (stage++) + ") : " + aggregationStage.toString());
            }

            AggregationOutput aggOutput = collection.aggregate(query);
            for (DBObject result : aggOutput.results()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("AggResult: " + result);
                }
                resultSet.add(MetaResultUtils.createRowWithAlias(result, select));
            }
        } catch (MongoException mongoException) {
            throw new MongoQueryException(mongoException.getMessage(), mongoException);
        }
        return resultSet;
    }

}