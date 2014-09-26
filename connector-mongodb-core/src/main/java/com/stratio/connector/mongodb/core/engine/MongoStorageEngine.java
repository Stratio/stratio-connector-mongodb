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
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * This class performs operations insert and delete in Mongo. Created by darroyo on 10/07/14.
 */
public class MongoStorageEngine extends CommonsStorageEngine<MongoClient> {

    private transient MongoConnectionHandler connectionHandler;

    /**
     * @param connectionHandler
     */
    public MongoStorageEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.engine.CommonsStorageEngine#insert(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.TableMetadata, com.stratio.meta.common.data.Row,
     * com.stratio.connector.commons.connection.Connection)
     */
    @Override
    public void insert(TableMetadata targetTable, Row row, Connection<MongoClient> connection)
                    throws UnsupportedException, ExecutionException {
        insert((MongoClient) connection.getNativeConnection(), targetTable, row);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.engine.CommonsStorageEngine#insert(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.TableMetadata, java.util.Collection,
     * com.stratio.connector.commons.connection.Connection)
     */
    @Override
    public void insert(TableMetadata targetTable, Collection<Row> rows, Connection<MongoClient> connection)
                    throws UnsupportedException, ExecutionException {

        insert(connection.getNativeConnection(), targetTable, rows);

    }

    /**
     * Insert a document in MongoDB.
     *
     * @param catalog
     *            the database.
     * @param tableName
     *            the collection.
     * @param row
     *            the row.
     * @throws ExecutionException
     *             in case of failure during the execution.
     */
    private void insert(MongoClient mongoClient, TableMetadata targetTable, Row row) throws ExecutionException,
                    UnsupportedException {

        String catalog = targetTable.getName().getCatalogName().getName();
        String tableName = targetTable.getName().getName();

        if (isEmpty(catalog) || isEmpty(tableName) || row == null) {
            throw new MongoInsertException("The catalog name, the table name and a row must be specified");
        } else {

            DB db = mongoClient.getDB(catalog);
            BasicDBObject doc = new BasicDBObject();

            Object pk = null; // TODO MongoDB use reflection?
            DBObject bsonPK = null;
            String cellName;
            Object cellValue;

            List<ColumnName> primaryKeyList = targetTable.getPrimaryKey();

            if (!primaryKeyList.isEmpty()) {
                if (primaryKeyList.size() == 1) {
                    cellValue = row.getCell(primaryKeyList.get(0).getName()).getValue();
                    validatePKDataType(cellValue, targetTable.getColumns().get(primaryKeyList.get(0)));
                    pk = cellValue;
                } else {

                    bsonPK = new BasicDBObject();
                    for (ColumnName columnName : primaryKeyList) {
                        cellValue = row.getCell(columnName.getName()).getValue();
                        validatePKDataType(cellValue, targetTable.getColumns().get(columnName));
                        // TODO columnName.getName() or "a", "b" .
                        bsonPK.put(columnName.getName(), cellValue);
                    }
                    pk = bsonPK;
                }

            }

            for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
                cellName = entry.getKey();
                cellValue = entry.getValue().getValue();
                ColumnName cName = new ColumnName(catalog, tableName, cellName);

                validateDataType(cellValue, targetTable.getColumns().get(cName));
                doc.put(entry.getKey(), cellValue);
            }

            if (pk != null) {
                // Upsert searching for _id
                BasicDBObject find = new BasicDBObject();
                find.put("_id", pk);
                db.getCollection(tableName).update(find, new BasicDBObject("$set", doc), true, false);
            } else {
                db.getCollection(tableName).insert(doc);
            }

        }

    }

    // if(key.contains(".") || metadata.contains(".")) {
    // //TODO throw new ExecutionException("The character '.' is not allowed"){};

    /**
     * @param cellValue
     * @param columnMetadata
     * @throws MongoInsertException
     */
    private void validateDataType(Object cellValue, ColumnMetadata columnMetadata) throws MongoInsertException {

        // TODO review with meta. cellValue instanceof is not checked
        ColumnType colType = columnMetadata.getColumnType();

        switch (colType) {
        case BIGINT:
            break;
        case BOOLEAN:
            break;
        case DOUBLE:
            break;
        case FLOAT:
            break;
        case INT:
            break;
        case LIST: // TODO isSupported?
            break;
        case MAP: // TODO isSupported?
            break;
        case NATIVE:
            // instanceof
            throw new MongoInsertException("Type not supported: " + colType.toString());
            // TODO if(columnMetadata.getParameters())
        case SET: // TODO isSupported?
            break;
        case TEXT:
            break;
        case VARCHAR:
            break;
        default:
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());

        }

    }

    /**
     * @param cellValue
     * @param columnMetadata
     * @throws MongoInsertException
     */
    private void validatePKDataType(Object cellValue, ColumnMetadata columnMetadata) throws MongoInsertException {
        // TODO review with meta. cellValue instanceof is not checked

        ColumnType colType = columnMetadata.getColumnType();
        switch (colType) {
        case BIGINT:
            break;
        case BOOLEAN:
            break;
        case DOUBLE:
            break;
        case FLOAT:
            break;
        case INT:
            break;
        case NATIVE:
            // instanceof
            // TODO if(columnMetadata.getParameters())
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());
        case LIST:
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());
        case MAP:
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());
        case SET:
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());
        case TEXT:
            break;
        case VARCHAR:
            break;
        default:
            throw new MongoInsertException("Type not supported as PK: " + colType.toString());

        }

    }

    // UPDATE??
    // http://docs.mongodb.org/manual/reference/operator/update/

    /**
     * Insert a set of documents in MongoDB.
     *
     * @param catalog
     *            the database.
     * @param tableName
     *            the collection.
     * @param row
     *            the row.
     * @throws ExecutionException
     *             in case of failure during the execution.
     */
    private void insert(MongoClient mongoClient, TableMetadata targetTable, Collection<Row> rows)
                    throws UnsupportedException, ExecutionException {

        for (Row row : rows) {
            insert(mongoClient, targetTable, row);
        }

    }

    /**
     * TODO UPDATE WHEN IFACE Delete a set of documents.
     * 
     * @param catalog
     *            the catalog.
     * 
     * @param tableName
     *            the collection.
     * 
     * @param filterSet
     *            filters to restrict the set of documents.
     */

    /*
     * private void delete(MongoClient mongoClient, String catalog, String tableName, Filter... filterSet) throws
     * UnsupportedOperationException { // TODO list Filter. And, Or, etc...
     * 
     * DB db = mongoClient.getDB(catalog);
     * 
     * if (db.collectionExists(tableName)) { DBCollection coll = db.getCollection(tableName); FilterDBObjectBuilder
     * filterBuilder = new FilterDBObjectBuilder(false);
     * 
     * for (Filter filter : filterSet) { filterBuilder.add(filter); }
     * 
     * coll.remove(filterBuilder.build()); }
     * 
     * }
     */

    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

}
