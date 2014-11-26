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

package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

// TODO: Auto-generated Javadoc
/**
 * The Class StorageUtils.
 *
 * @author david
 */
public final class StorageUtils {

    /**
     * Instantiates a new storage utils.
     */
    private StorageUtils() {
    }

    /**
     * Builds the primaryKey. Only the value will be stored when receiving a single primaryKey. Otherwise, either the
     * name or the value will be stored.
     *
     * @param tableMetadata
     *            the table metadata
     * @param row
     *            the row
     * @return the primary key value
     * @throws MongoValidationException
     *             if the specified primary key is not valid
     */
    public static Object buildPK(TableMetadata tableMetadata, Row row) throws MongoValidationException {

        Object pk = null;
        DBObject bsonPK = null;
        Object cellValue;

        List<ColumnName> primaryKeyList = tableMetadata.getPrimaryKey();

        // Building the pk to insert in Mongo
        if (!primaryKeyList.isEmpty()) {
            if (primaryKeyList.size() == 1) {
                cellValue = row.getCell(primaryKeyList.get(0).getName()).getValue();
                validatePKDataType(tableMetadata.getColumns().get(primaryKeyList.get(0)).getColumnType());
                pk = cellValue;
            } else {

                bsonPK = new BasicDBObject();
                for (ColumnName columnName : primaryKeyList) {
                    cellValue = row.getCell(columnName.getName()).getValue();
                    validatePKDataType(tableMetadata.getColumns().get(columnName).getColumnType());
                    bsonPK.put(columnName.getName(), cellValue);
                }
                pk = bsonPK;
            }
        }

        return pk;
    }

    /**
     * Checks if the column type is supported in primary keys.
     *
     * @param columnType
     *            the columnType type
     * @param cellValue
     *            the cell value
     * @throws MongoValidationException
     *             if the type is not supported
     */
    private static void validatePKDataType(ColumnType columnType) throws MongoValidationException {

        switch (columnType) {
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
            throw new MongoValidationException("Type not supported as PK: " + columnType.toString());

        }

    }
    

}
