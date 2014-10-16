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
package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.meta.common.data.Row;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * @author david
 *
 */
public class StorageUtils {

    public static Object buildPK(TableMetadata targetTable, Row row) throws MongoValidationException {

        Object pk = null;
        DBObject bsonPK = null;
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

        return pk;
    }

    private static void validatePKDataType(ColumnType columnType) throws MongoValidationException {
        validatePKDataType(columnType, null);

    }

    private static void validatePKDataType(ColumnType colType, Object cellValue) throws MongoValidationException {

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
}