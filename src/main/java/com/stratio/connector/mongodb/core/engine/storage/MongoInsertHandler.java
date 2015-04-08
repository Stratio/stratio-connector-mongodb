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
package com.stratio.connector.mongodb.core.engine.storage;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * The class handles the different inserts in Mongo.
 */
public class MongoInsertHandler {

    /** The logger. */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /** The MongoDB collection. */
    private DBCollection collection;

    /** The bulk write operation. */
    private BulkWriteOperation bulkWriteOperation;

    /**
     * Instantiates a new mongo insert handler.
     *
     * @param collection
     *            the collection
     */
    public MongoInsertHandler(DBCollection collection) {
        this.collection = collection;
    }

    /**
     * Start batch.
     */
    public void startBatch() {
        bulkWriteOperation = collection.initializeUnorderedBulkOperation();

    }

    /**
     * Insert if not exist.
     *
     * @param targetTable
     *            the target table
     * @param row
     *            the row
     * @param pk
     *            the pk
     * @throws MongoValidationException
     *             if the operation is not supported by the connector
     * @throws MongoInsertException
     *             if the insertion fails
     */
    public void insertIfNotExist(TableMetadata targetTable, Row row, Object pk) throws MongoValidationException,
                    MongoInsertException {

        DBObject doc = getBSONFromRow(targetTable, row);
        BasicDBObject find = new BasicDBObject("_id", pk);

        try {
            if (bulkWriteOperation != null) {
                bulkWriteOperation.find(find).upsert().update(new BasicDBObject("$setOnInsert", doc));
            } else {
                collection.update(find, new BasicDBObject("$setOnInsert", doc), true, false);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Row updated with fields: " + doc.keySet());
            }
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }
    }

    /**
     * Upsert.
     *
     * @param targetTable
     *            the target table
     * @param row
     *            the row
     * @param pk
     *            the pk
     * @throws MongoInsertException
     *             if the insertion fails
     * @throws MongoValidationException
     *             if the operation is not supported by the connector
     */
    public void upsert(TableMetadata targetTable, Row row, Object pk) throws MongoInsertException,
                    MongoValidationException {
        // Upsert searching for _id
        DBObject doc = getBSONFromRow(targetTable, row);
        BasicDBObject find = new BasicDBObject("_id", pk);

        try {
            if (bulkWriteOperation != null) {
                bulkWriteOperation.find(find).upsert().update(new BasicDBObject("$set", doc));
            } else {
                collection.update(find, new BasicDBObject("$set", doc), true, false);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Row updated with fields: " + doc.keySet());
            }
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }
    }

    /**
     * Insert without pk.
     *
     * @param targetTable
     *            the target table
     * @param row
     *            the row
     * @throws MongoValidationException
     *             if the operation is not supported by the connector
     * @throws MongoInsertException
     *             if the insertion fails
     */
    public void insertWithoutPK(TableMetadata targetTable, Row row) throws MongoValidationException,
                    MongoInsertException {

        DBObject doc = getBSONFromRow(targetTable, row);
        try {
            if (bulkWriteOperation != null) {
                bulkWriteOperation.insert(doc);
            } else {
                collection.insert(doc);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Row inserted with fields: " + doc.keySet());
            }
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }

    }

    /**
     * Execute batch.
     *
     * @throws MongoInsertException
     *             the mongo insert exception
     */
    public void executeBatch() throws MongoInsertException {
        try {
            bulkWriteOperation.execute();
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }

    }

    // Building the fields to insert in Mongo
    private DBObject getBSONFromRow(TableMetadata targetTable, Row row) throws MongoValidationException {

        String catalog = targetTable.getName().getCatalogName().getName();
        String tableName = targetTable.getName().getName();
        BasicDBObject doc = new BasicDBObject();
        String cellName;
        Object cellValue;

        for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
            cellName = entry.getKey();
            cellValue = entry.getValue().getValue();
            ColumnName cName = new ColumnName(catalog, tableName, cellName);
            validateDataType(targetTable.getColumns().get(cName).getColumnType());
            doc.put(entry.getKey(), cellValue);
        }
        return doc;
    }

    /**
     * Validates the data type.
     *
     * @param colType
     *            the column type
     * @throws MongoValidationException
     *             if the type is not supported
     */
    private void validateDataType(ColumnType columnType) throws MongoValidationException {

        switch (columnType.getDataType()) {
        case BIGINT:
        case BOOLEAN:
        case INT:
        case TEXT:
        case VARCHAR:
        case DOUBLE:
        case FLOAT:
            break;
        case SET:
        case LIST:
            validateDataType(columnType.getDBInnerType());
            break;
        case MAP:
            validateDataType(columnType.getDBInnerType());
            validateDataType(columnType.getDBInnerValueType());
            break;
        case NATIVE:
        default:
            throw new MongoValidationException("Type not supported: " + columnType.toString());
        }

    }

}
