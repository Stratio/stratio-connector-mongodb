/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.mongodb.core.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

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
     * 
     * @throws ExecutionException
     *             in case of failure during the execution.
     */
    @Override
    protected void insert(TableMetadata targetTable, Row row, Connection<MongoClient> connection)
                    throws UnsupportedException, ExecutionException {

        MongoClient mongoClient = connection.getNativeConnection();

        String catalog = targetTable.getName().getCatalogName().getName();
        String tableName = targetTable.getName().getName();

        if (isEmpty(catalog) || isEmpty(tableName) || row == null) {
            throw new MongoInsertException("The catalog name, the table name and a row must be specified");
        } else {

            DB db = mongoClient.getDB(catalog);
            BasicDBObject doc = new BasicDBObject();

            Object pk = null;
            DBObject bsonPK = null;
            String cellName;
            Object cellValue;

            List<ColumnName> primaryKeyList = targetTable.getPrimaryKey();

            // Building the pk to insert in Mongo
            if (!primaryKeyList.isEmpty()) {
                if (primaryKeyList.size() == 1) {
                    cellValue = row.getCell(primaryKeyList.get(0).getName()).getValue();
                    validatePKDataType(targetTable.getColumns().get(primaryKeyList.get(0)).getColumnType());
                    pk = cellValue;
                } else {

                    bsonPK = new BasicDBObject();
                    for (ColumnName columnName : primaryKeyList) {
                        cellValue = row.getCell(columnName.getName()).getValue();
                        validatePKDataType(targetTable.getColumns().get(columnName).getColumnType());
                        bsonPK.put(columnName.getName(), cellValue);
                    }
                    pk = bsonPK;
                }

            }

            // Building the fields to insert in Mongo
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
     * 
     * @throws ExecutionException
     *             in case of failure during the execution.
     * @throws UnsupportedException
     *             in case of the operation is not supported
     */
    @Override
    protected void insert(TableMetadata targetTable, Collection<Row> rows, Connection<MongoClient> connection)
                    throws UnsupportedException, ExecutionException {

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

    private void validatePKDataType(ColumnType columnType) throws MongoValidationException {
        validatePKDataType(columnType, null);

    }

    private void validatePKDataType(ColumnType colType, Object cellValue) throws MongoValidationException {

        switch (colType) {
        case BIGINT:
        case INT:
        case TEXT:
        case VARCHAR:
        case DOUBLE:
        case FLOAT:
            break;
        case BOOLEAN:
        case SET:
        case LIST:
        case MAP:
        case NATIVE:
        default:
            throw new MongoValidationException("Type not supported as PK: " + colType.toString());

        }

    }

    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

}
