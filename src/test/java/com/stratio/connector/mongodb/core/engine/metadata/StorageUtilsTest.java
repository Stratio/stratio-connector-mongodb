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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.mongodb.DBObject;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * @author david
 */
public class StorageUtilsTest {

    private static final String COLLECTION_NAME = "coll_name";
    private static final String DB_NAME = "db_name";
    private static final String ROW_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "other_row_name";
    private static final String CELL_VALUE = "cell_value";
    private static final Object OTHER_CELL_VALUE = "othercellvalue";
    private static final Integer INTEGER_CELL_VALUE = new Integer(5);
    private static final ColumnType VARCHAR_COLUMN_TYPE = ColumnType.VARCHAR;
    private static final ColumnType INT_COLUMN_TYPE = ColumnType.INT;

    @Test
    public void buildPKTest() throws MongoValidationException {

        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        tableMetaBuilder.addColumn(ROW_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_ROW_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(ROW_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(ROW_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_ROW_NAME, new Cell(OTHER_CELL_VALUE));

        Object pk = StorageUtils.buildPK(tableMetadata, row);

        assertEquals("The value is not the expected", CELL_VALUE, (String) pk);

    }

    @Test
    public void buildCompoundPKTest() throws MongoValidationException {

        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        tableMetaBuilder.addColumn(ROW_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_ROW_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(ROW_NAME);
        tableMetaBuilder.withClusterKey(OTHER_ROW_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(ROW_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_ROW_NAME, new Cell(INTEGER_CELL_VALUE));

        DBObject pk = (DBObject) StorageUtils.buildPK(tableMetadata, row);

        assertTrue("The primary key must contain " + ROW_NAME, pk.containsField(ROW_NAME));
        assertTrue("The primary key must contain " + OTHER_ROW_NAME, pk.containsField(OTHER_ROW_NAME));
        assertEquals("The string value is not the expected", CELL_VALUE, pk.get(ROW_NAME));
        assertEquals("The integer value is not the expected", INTEGER_CELL_VALUE, pk.get(OTHER_ROW_NAME));

    }

    @Test(expected = MongoValidationException.class)
    public void buildMalformedPKTest() throws MongoValidationException {

        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        tableMetaBuilder.addColumn(ROW_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_ROW_NAME, ColumnType.SET);
        tableMetaBuilder.withPartitionKey(OTHER_ROW_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();

        Row row = new Row();
        row.addCell(OTHER_ROW_NAME, new Cell(Collections.EMPTY_SET));

        StorageUtils.buildPK(tableMetadata, row);

    }
}
