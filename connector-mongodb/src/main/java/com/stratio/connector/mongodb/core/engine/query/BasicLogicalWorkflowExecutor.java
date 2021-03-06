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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.engine.query.utils.MetaResultUtils;
import com.stratio.connector.mongodb.core.exceptions.MongoExecutionException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.DEFAULT_LIMIT;

/**
 * The LogicalWorkflowExecutor for non-aggregation queries.
 */
public class BasicLogicalWorkflowExecutor extends LogicalWorkflowExecutor {

    /**
     * Instantiates a new basic logical workflow executor.
     *
     * @param logicalWorkflowParsed
     *            the logical workflow parsed
     * @throws ExecutionException
     *             if the execution fails or the query specified in the logical workflow is not supported
     * @throws UnsupportedException
     *             if the specified operation is not supported.
     */
    public BasicLogicalWorkflowExecutor(ProjectParsed logicalWorkflowParsed) throws ExecutionException,
                    UnsupportedException {
        super(logicalWorkflowParsed);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.mongodb.core.engine.query.LogicalWorkflowExecutor#buildQuery()
     */
    @Override
    protected void buildQuery() throws MongoValidationException, UnsupportedException {
        query = new ArrayList<DBObject>();
        query.add(buildFilter(false));

    }

    /**
     * Execute an usual query.
     *
     * @param connection
     *            the Connection that holds the mongodb client.
     * @return the Crossdata ResultSet.
     * @throws MongoValidationException .
     * @throws ExecutionException
     *             if the execution fails or the query specified in the logical workflow is not supported.
     */
    public ResultSet executeQuery(Connection<MongoClient> connection) throws ExecutionException {

        MongoClient mongoClient = connection.getNativeConnection();
        DB db = mongoClient.getDB(logicalWorkflowData.getProject().getCatalogName());
        DBCollection collection = db.getCollection(logicalWorkflowData.getProject().getTableName().getName());
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(MetaResultUtils.createMetadata(logicalWorkflowData.getProject(),
                        logicalWorkflowData.getSelect()));

        if (logger.isDebugEnabled()) {
            logger.debug("Executing MongoQuery: " + query.get(0) + ", with fields: " + buildProject(false));
        }

        DBCursor cursor = collection.find(query.get(0), buildProject(false));

        if (logicalWorkflowData.getOrderBy() != null) {
            cursor = cursor.sort(buildOrderBy(false));
        }

        if (logicalWorkflowData.getLimit() != null) {
            cursor = cursor.limit(logicalWorkflowData.getLimit().getLimit());
        }else{
            String limit = connection.getSessionObject(String.class, DEFAULT_LIMIT.getOptionName());
            cursor = cursor.limit(Integer.parseInt(limit));
        }

        DBObject rowDBObject;
        try {
            while (cursor.hasNext()) {
                rowDBObject = cursor.next();
                if (logger.isDebugEnabled()) {
                    logger.debug("BResult: " + rowDBObject);
                }
                resultSet.add(MetaResultUtils.createRowWithAlias(rowDBObject, logicalWorkflowData.getSelect()));
            }
        } catch (MongoException e) {
            logger.error("Error executing a basic query :" + query.get(0) + ", with fields: " + buildProject(false)
                            + "\n Error:" + e.getMessage());
            throw new MongoExecutionException(e.getMessage(), e);
        } finally {
            cursor.close();
        }

        return resultSet;
    }

}
