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

package com.stratio.connector.mongodb.core.engine.query.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.DBObject;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;

/**
 * The utility class MetaResultUtils.
 *
 */
public final class MetaResultUtils {

    private MetaResultUtils() {
    }

    /**
     * Creates a row with alias from a Mongo result. A null value is inserted if any field of the row is missing.
     *
     * @param rowDBObject
     *            a bson containing the result.
     * @param select
     *            the select
     * @return the row.
     */
    public static Row createRowWithAlias(DBObject rowDBObject, Select select) {

        Row row = new Row();
        Map<ColumnName, String> aliasMapping = select.getColumnMap();

        String field;
        for (Entry<ColumnName, String> colInfo : aliasMapping.entrySet()) {
            field = colInfo.getKey().getName();
            Object value = rowDBObject.get(field);

            value = castValue(value, select.getTypeMapFromColumnName().get(colInfo.getKey()));
            if (colInfo.getValue() != null) {
                field = colInfo.getValue();
            }

            row.addCell(field, new Cell(value));
        }

        return row;
    }

    /**
     * Cast the value according to column type.
     *
     * @param value
     *            the value casted
     * @param columnType
     *            the column type
     */
    private static Object castValue(Object value, ColumnType columnType) {
        Object castedValue = value;
        switch (columnType) {
        case FLOAT:
            castedValue = ((Double) value).floatValue();
            break;
        case SET:
        case LIST:
        case MAP:
            break;
        default:
            break;
        }
        return castedValue;

    }

    /**
     * Creates the column metadata.
     *
     * @param projection
     *            the projection
     * @param select
     *            the select
     * @return the columns metadata
     */
    public static List<ColumnMetadata> createMetadata(Project projection, Select select) {
        List<ColumnMetadata> columnsMetadata = new ArrayList<>();
        for (ColumnName colName : select.getColumnMap().keySet()) {

            ColumnType colType = select.getTypeMapFromColumnName().get(colName);

            colType = updateColumnType(colType);

            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getQualifiedName(),
                            colName.getQualifiedName(), colType);
            columnMetadata.setColumnAlias(select.getColumnMap().get(colName));

            columnsMetadata.add(columnMetadata);

        }
        return columnsMetadata;
    }

    /**
     * Update column type.
     *
     * @param colType
     *            the col type
     * @return the column type
     */
    private static ColumnType updateColumnType(ColumnType colType) {
        String dbType;
        switch (colType) {
        case FLOAT:
            break;
        case SET:
        case LIST:
            dbType = colType.getODBCType();
            colType.setDBMapping(dbType, List.class);
            colType.setDBCollectionType(updateColumnType(colType.getDBInnerType()));
            break;
        case MAP:
            dbType = colType.getODBCType();
            colType.setDBMapping(dbType, Map.class);
            colType.setDBMapType((updateColumnType(colType.getDBInnerType())),
                            updateColumnType(colType.getDBInnerValueType()));
            break;
        case BIGINT:
        case BOOLEAN:
        case DOUBLE:
        case INT:
        case TEXT:
        case VARCHAR:
        default:
            break;
        }
        return colType;

    }
}
