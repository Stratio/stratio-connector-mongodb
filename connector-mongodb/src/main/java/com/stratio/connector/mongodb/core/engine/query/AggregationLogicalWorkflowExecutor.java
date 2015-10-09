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
import java.util.Collections;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.engine.query.utils.LimitDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.MetaResultUtils;
import com.stratio.connector.mongodb.core.exceptions.MongoExecutionException;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.metadata.Operations;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.DEFAULT_LIMIT;

/**
 * The LogicalWorkflowExecutor for the MongoDB aggregation framework.
 */
public class AggregationLogicalWorkflowExecutor extends LogicalWorkflowExecutor {

    /**
     * Instantiates a new aggregation logical workflow executor.
     *
     * @param logicalWorkflowParsed
     *            the logical workflow parsed
     * @throws ExecutionException
     *             if the execution fails or if the query specified in the logical workflow is not supported
     * @throws UnsupportedException
     *             if the specified operation is not supported.
     */
    public AggregationLogicalWorkflowExecutor(ProjectParsed logicalWorkflowParsed) throws ExecutionException,
                    UnsupportedException {
        super(logicalWorkflowParsed);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutor#buildQuery()
     */
    @Override
    protected void buildQuery() throws ExecutionException, UnsupportedException {
        query = new ArrayList<DBObject>();

        if (logicalWorkflowData.getProject() != null) {
            query.add(buildProject(true));
        }
        if (!logicalWorkflowData.getFilter().isEmpty()) {
            query.add(buildFilter(true));
        }
        if (logicalWorkflowData.getGroupBy() != null) {
            query.add(buildGroupBy());
        }
        if (logicalWorkflowData.getOrderBy() != null) {
            query.add(buildOrderBy(true));
        }
        if (logicalWorkflowData.getLimit() != null) {
            query.add(buildLimit());
        }

    }

    /**
     * Execute an aggregation query.
     *
     * @param connection
     *            the Connection that holds the MongoDB client.
     * @return the Crossdata ResultSet.
     * @throws ExecutionException
     *             if the execution fails.
     */
    @Override
    public ResultSet executeQuery(Connection<MongoClient> connection) throws ExecutionException {
        MongoClient mongoClient = connection.getNativeConnection();

        DB db = mongoClient.getDB(logicalWorkflowData.getProject().getCatalogName());
        DBCollection collection = db.getCollection(logicalWorkflowData.getProject().getTableName().getName());
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(MetaResultUtils.createMetadata(logicalWorkflowData.getProject(),
                        logicalWorkflowData.getSelect()));

        if (logicalWorkflowData.getLimit() == null){
            int limit =  Integer.parseInt(connection.getSessionObject(String.class, DEFAULT_LIMIT.getOptionName()));
            Limit limitObj = new Limit(Collections.singleton(Operations.SELECT_GROUP_BY),limit);
            query.add(new LimitDBObjectBuilder(limitObj).build());
        }
        // AggregationOptions aggOptions = AggregationOptions.builder()
        // .allowDiskUse(true)
        // .batchSize(size)
        // pipeline,aggOptions => dbcursor
        try {
            int stage = 1;
            for (DBObject aggregationStage : query) {
                logger.debug("Aggregate framework stage (" + (stage++) + ") : " + aggregationStage);
            }
            AggregationOutput aggOutput = collection.aggregate(query);
            for (DBObject result : aggOutput.results()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("AggResult: " + result);
                }
                resultSet.add(MetaResultUtils.createRowWithAlias(result, logicalWorkflowData.getSelect()));
            }
        } catch (MongoException mongoException) {
            logger.error("Error executing an aggregation query:" + mongoException.getMessage());
            throw new MongoExecutionException(mongoException.getMessage(), mongoException);
        }

        return resultSet;
    }

}
