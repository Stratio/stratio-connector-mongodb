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
import java.util.Map;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.StorageUtils;
import com.stratio.connector.mongodb.core.engine.metadata.UpdateDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoDeleteException;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * This class performs insert and delete operations in Mongo.
 */
public class MongoStorageEngine extends CommonsStorageEngine<MongoClient> {

    /**
     * Instantiates a new mongo storage engine.
     *
     * @param connectionHandler
     *            the connection handler
     */
    public MongoStorageEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
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
    @Override
    protected void insert(TableMetadata targetTable, Collection<Row> rows, Connection<MongoClient> connection)
                    throws MongoInsertException, MongoValidationException {

        for (Row row : rows) {
            insert(targetTable, row, connection);
        }

    }

    /**
     * Validates the data type.
     *
     * @param colType
     *            the column type
     * @param cellValue
     *            the cell value
     * @throws MongoValidationException
     *             if the type is not supported
     */
    private void validateDataType(ColumnType columnType) throws MongoValidationException {

        switch (columnType) {
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
            throw new MongoValidationException("Type not supported: " + columnType.toString());

        default:
            throw new MongoValidationException("Type not supported: " + columnType.toString());
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

	@Override
	protected void truncate(TableName tableName,
			Connection<MongoClient> connection) throws UnsupportedException,
			ExecutionException {
	    delete(tableName, null,connection);

	}

	@Override
	protected void delete(TableName tableName, Collection<Filter> whereClauses,
			Connection<MongoClient> connection) throws UnsupportedException,
			ExecutionException {
		
		DB db = connection.getNativeConnection().getDB(tableName.getCatalogName().getName());
		if (db.collectionExists(tableName.getName())) {
			DBCollection coll = db.getCollection(tableName.getName());
	
			coll.remove(buildFilter(whereClauses));
		}
		
	}

	@Override
	protected void update(TableName tableName,
			Collection<Relation> assignments, Collection<Filter> whereClauses,
			Connection<MongoClient> connection) throws UnsupportedException,
			ExecutionException {
		
		DB db = connection.getNativeConnection().getDB(tableName.getCatalogName().getName());
		DBCollection coll = db.getCollection(tableName.getName());
		
		UpdateDBObjectBuilder updateBuilder = new UpdateDBObjectBuilder();
		for(Relation rel: assignments){
		    updateBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(), rel.getRightTerm());
		}
		coll.update(buildFilter(whereClauses), updateBuilder.build(), false, true);
		
	}
	
	private DBObject buildFilter(Collection<Filter> whereClauses) throws MongoValidationException{
		List<Filter> filters;
		if(whereClauses == null){
		    return new BasicDBObject();
		}else{
    		if(whereClauses instanceof List){
    			filters = (List<Filter>) whereClauses;
    		}else{
    			filters = new ArrayList<Filter>(whereClauses);
    		}
    		FilterDBObjectBuilder filterBuilder = new FilterDBObjectBuilder(
    				false,filters);
    		return filterBuilder.build();
	    }
	}

}
