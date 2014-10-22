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
package com.stratio.connector.mongodb.core.engine;

import java.util.Collection;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.StorageUtils;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * This class performs operations insert and delete in Mongo. Created by darroyo on 10/07/14.
 */
public class MongoStorageEngine extends CommonsStorageEngine<MongoClient> {

    /**
     * @param connectionHandler
     */
    public MongoStorageEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * Insert a document in MongoDB.
     *
     * 
     * @param targetTable
     *            the table metadata.
     * @param row
     *            the row.
     * @param connection
     * @throws MongoInsertException
     * @throws MongoValidationException
     * 
     * @throws ExecutionException
     *             in case of failure during the execution.
     */
    @Override
    protected void insert(TableMetadata targetTable, Row row, Connection<MongoClient> connection)
                    throws MongoInsertException, MongoValidationException {

        MongoClient mongoClient = connection.getNativeConnection();

        String catalog = targetTable.getName().getCatalogName().getName();
        String tableName = targetTable.getName().getName();

        if (isEmpty(catalog) || isEmpty(tableName) || row == null) {
            throw new MongoInsertException("The catalog name, the table name and a row must be specified");
        }

        DB db = mongoClient.getDB(catalog);
        Object pk = StorageUtils.buildPK(targetTable, row);

        // Building the fields to insert in Mongo
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

        if (pk != null) {
            // Upsert searching for _id
            BasicDBObject find = new BasicDBObject();
            find.put("_id", pk);
            try {
                db.getCollection(tableName).update(find, new BasicDBObject("$set", doc), true, false);
            } catch (MongoException e) {
                throw new MongoInsertException(e.getMessage(), e);
            }
        } else {
            try {
                db.getCollection(tableName).insert(doc);
            } catch (MongoException e) {
                throw new MongoInsertException(e.getMessage(), e);
            }
        }

    }

    /**
     * Insert a document in MongoDB.
     *
     * 
     * @param targetTable
     *            the table metadata.
     * @param rows
     *            the set of rows.
     * @param connection
     * @throws MongoValidationException
     * @throws MongoInsertException
     * 
     * @throws ExecutionException
     *             in case of failure during the execution.
     * @throws UnsupportedException
     *             in case of the operation is not supported
     */
    @Override
    protected void insert(TableMetadata targetTable, Collection<Row> rows, Connection<MongoClient> connection)
                    throws MongoInsertException, MongoValidationException {

        for (Row row : rows) {
            insert(targetTable, row, connection);
        }

    }

    /**
     * @param columnType
     * @throws MongoInsertException
     */
    private void validateDataType(ColumnType columnType) throws MongoValidationException {
        validateDataType(columnType, null);

    }

    /**
     * @param cellValue
     * @param columnMetadata
     * @throws MongoInsertException
     * @throws MongoValidationException
     */
    private void validateDataType(ColumnType colType, Object cellValue) throws MongoValidationException {

        // TODO review with meta.
        switch (colType) {
        case BIGINT:
        case BOOLEAN:
        case INT:
        case TEXT:
        case VARCHAR:
        case DOUBLE:
        case FLOAT:
            break;
        case SET: // TODO isSupported?
        case LIST:
            validateDataType(colType.getDBInnerType());
            break;
        case MAP: // TODO isSupported?
            validateDataType(colType.getDBInnerType());
            validateDataType(colType.getDBInnerValueType());
            break;
        case NATIVE:
            throw new MongoValidationException("Type not supported: " + colType.toString());
            // // TODO if (!NativeTypes.DATE.getDbType().equals(colType.getDbType()))
            // if (!(cellValue instanceof Date))
            // throw new MongoInsertException("Type not supported: " + colType.toString());
            // // TODO if(columnMetadata.getParameters())
        default:
            throw new MongoValidationException("Type not supported: " + colType.toString());
        }

    }

    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

}
