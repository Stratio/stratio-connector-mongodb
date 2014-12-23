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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.StorageUtils;
import com.stratio.connector.mongodb.core.engine.metadata.UpdateDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.storage.MongoInsertHandler;
import com.stratio.connector.mongodb.core.exceptions.MongoDeleteException;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * This class performs insert and delete operations in Mongo.
 */
public class MongoStorageEngine extends CommonsStorageEngine<MongoClient> {

    /** The logger. */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Instantiates a new MongoDB storage engine.
     *
     * @param connectionHandler
     *            the connection handler
     */
    public MongoStorageEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    protected void insert(TableMetadata targetTable, Row row, boolean isNotExists, Connection<MongoClient> connection)
                    throws UnsupportedException, ExecutionException {
        // TODO Auto-generated method stub

        if (isNotExists) {
            throw new UnsupportedException("Insert if not exist not supported");
        } else {
            insert(targetTable, row, connection);
        }
    }

    /**
     * Inserts a document in MongoDB.
     *
     * @param targetTable
     *            the table metadata
     * @param row
     *            the row. it will be the MongoDB document
     * @param connection
     *            the connection
     * @throws MongoInsertException
     *             if an error exist when inserting data
     * @throws MongoValidationException
     *             if the specified operation is not supported
     */

    private void insert(TableMetadata targetTable, Row row, Connection<MongoClient> connection)
                    throws MongoInsertException, MongoValidationException {

        MongoClient mongoClient = connection.getNativeConnection();
        DBCollection collection = mongoClient.getDB(targetTable.getName().getCatalogName().getName()).getCollection(
                        targetTable.getName().getName());

        MongoInsertHandler insertHandler = new MongoInsertHandler(collection);
        validateInsert(targetTable, row);

        Object pk = StorageUtils.buildPK(targetTable, row);

        if (pk != null) {
            insertHandler.upsert(targetTable, row, pk);
        } else {
            insertHandler.insertWithoutPK(targetTable, row);
        }

    }

    @Override
    protected void insert(TableMetadata targetTable, Collection<Row> rows, boolean isNotExists,
                    Connection<MongoClient> connection) throws UnsupportedException, ExecutionException {
        // TODO Auto-generated method stub
        if (isNotExists) {
            throw new UnsupportedException("Insert if not exist not supported");
        } else {
            insert(targetTable, rows, connection);
        }

    }

    /**
     * Inserts a collection of documents in MongoDB.
     *
     * @param targetTable
     *            the table metadata.
     * @param rows
     *            the set of rows.
     * @param connection
     *            the connection
     * @throws MongoInsertException
     *             if an error exist when inserting data
     * @throws MongoValidationException
     *             if the specified operation is not supported
     */
    private void insert(TableMetadata targetTable, Collection<Row> rows, Connection<MongoClient> connection)
                    throws MongoInsertException, MongoValidationException {

        MongoClient mongoClient = connection.getNativeConnection();
        DBCollection collection = mongoClient.getDB(targetTable.getName().getCatalogName().getName()).getCollection(
                        targetTable.getName().getName());

        MongoInsertHandler insertHandler = new MongoInsertHandler(collection);
        insertHandler.startBatch();

        for (Row row : rows) {
            validateInsert(targetTable, row);
            Object pk = StorageUtils.buildPK(targetTable, row);
            if (pk != null) {
                insertHandler.upsert(targetTable, row, pk);
            } else {
                insertHandler.insertWithoutPK(targetTable, row);
            }
        }
        insertHandler.executeBatch();

    }

    @Override
    protected void truncate(TableName tableName, Connection<MongoClient> connection) throws ExecutionException {
        delete(tableName, null, connection);

    }

    @Override
    protected void delete(TableName tableName, Collection<Filter> whereClauses, Connection<MongoClient> connection)
                    throws MongoValidationException, ExecutionException {

        DB db = connection.getNativeConnection().getDB(tableName.getCatalogName().getName());
        if (db.collectionExists(tableName.getName())) {
            DBCollection coll = db.getCollection(tableName.getName());

            try {
                coll.remove(buildFilter(whereClauses));
            } catch (MongoException e) {
                logger.error("Error deleting the data: " + e.getMessage());
                throw new MongoDeleteException(e.getMessage(), e);
            }

        }

    }

    @Override
    protected void update(TableName tableName, Collection<Relation> assignments, Collection<Filter> whereClauses,
                    Connection<MongoClient> connection) throws MongoValidationException, ExecutionException {

        DB db = connection.getNativeConnection().getDB(tableName.getCatalogName().getName());
        DBCollection coll = db.getCollection(tableName.getName());

        UpdateDBObjectBuilder updateBuilder = new UpdateDBObjectBuilder();
        for (Relation rel : assignments) {
            updateBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(), rel.getRightTerm());
        }
        try {
            coll.update(buildFilter(whereClauses), updateBuilder.build(), false, true);
        } catch (MongoException e) {
            logger.error("Error updating the data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }

    }

    private DBObject buildFilter(Collection<Filter> whereClauses) throws MongoValidationException {
        List<Filter> filters;
        if (whereClauses == null) {
            return new BasicDBObject();
        } else {
            if (whereClauses instanceof List) {
                filters = (List<Filter>) whereClauses;
            } else {
                filters = new ArrayList<Filter>(whereClauses);
            }
            FilterDBObjectBuilder filterBuilder = new FilterDBObjectBuilder(false, filters);
            return filterBuilder.build();
        }
    }

    private void validateInsert(TableMetadata targetTable, Row row) throws MongoValidationException {
        if (isEmpty(targetTable.getName().getCatalogName().getName()) || isEmpty(targetTable.getName().getName())
                        || row == null) {
            throw new MongoValidationException("The catalog name, the table name and the row must be specified");
        }
    }

    /**
     * Checks if is empty.
     *
     * @param value
     *            the value
     * @return true, if is empty
     */
    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

}
