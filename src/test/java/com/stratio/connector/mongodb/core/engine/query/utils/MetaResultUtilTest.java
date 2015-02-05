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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

public class MetaResultUtilTest {

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String ALIAS_COLUMN_1 = COLUMN_1 + "alias";
    public static final String ALIAS_COLUMN_2 = COLUMN_2 + "alias";
    public static final String ALIAS_COLUMN_3 = COLUMN_3 + "alias";
    public static final String COLUMNS_VALUE = "value";
    public static final String OTHER_COLUMN_VALUE = "other_value";
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    @Test
    public void createRowWithAliasTest() throws ExecutionException {
        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, ColumnType.VARCHAR), new ConnectorField(
                        COLUMN_2, ALIAS_COLUMN_2, ColumnType.VARCHAR), new ConnectorField(COLUMN_3, ALIAS_COLUMN_3,
                        ColumnType.VARCHAR));
        Select select = getSelect(columns);

        DBObject rowDBObject = new BasicDBObject(COLUMN_1, COLUMNS_VALUE);
        rowDBObject.put(COLUMN_2, OTHER_COLUMN_VALUE);

        Row rowWithAlias = MetaResultUtils.createRowWithAlias(rowDBObject, select);

        // Checking the number of columns returned
        Assert.assertEquals("There should be 3 columns", 3, rowWithAlias.size());

        // Checking the alias and the value
        Assert.assertEquals(ALIAS_COLUMN_1 + " should be " + COLUMNS_VALUE, COLUMNS_VALUE,
                        rowWithAlias.getCell(ALIAS_COLUMN_1).getValue());
        Assert.assertEquals(ALIAS_COLUMN_2 + " should be " + OTHER_COLUMN_VALUE, OTHER_COLUMN_VALUE, rowWithAlias
                        .getCell(ALIAS_COLUMN_2).getValue());
        Assert.assertNull(ALIAS_COLUMN_3 + " should be null", rowWithAlias.getCell(ALIAS_COLUMN_3).getValue());

    }

    @Test
    public void createMetadataTest() throws MongoValidationException {
        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, ColumnType.VARCHAR), new ConnectorField(
                        COLUMN_2, ALIAS_COLUMN_2, ColumnType.VARCHAR), new ConnectorField(COLUMN_3, ALIAS_COLUMN_3,
                        ColumnType.VARCHAR));
        Select select = getSelect(columns);

        Project project = Mockito.mock(Project.class);
        Mockito.when(project.getCatalogName()).thenReturn(CATALOG);
        Mockito.when(project.getTableName()).thenReturn(new TableName(CATALOG, TABLE));

        List<ColumnMetadata> columnsMetadata = MetaResultUtils.createMetadata(project, select);

        Assert.assertEquals("The number of metadata columns must be equal than the selected column", select
                        .getColumnMap().size(), columnsMetadata.size());

        Assert.assertEquals("The name is not the expected", COLUMN_1, columnsMetadata.get(0).getName().getName());
        Assert.assertEquals("The type is not the expected", ColumnType.VARCHAR, columnsMetadata.get(0).getColumnType());
        Assert.assertEquals("The alias is not the expected", ALIAS_COLUMN_1, columnsMetadata.get(0).getName()
                        .getAlias());

    }

    private Select getSelect(List<ConnectorField> fields) {
        Select select;
        Map<Selector, String> mapping = new LinkedHashMap<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        Map<Selector, ColumnType> typeMapFormColumnName = new LinkedHashMap<>();

        for (ConnectorField connectorField : fields) {
            ColumnSelector columnSelector = new ColumnSelector(new ColumnName(CATALOG, TABLE, connectorField.name));
            mapping.put(columnSelector, connectorField.alias);
            types.put(connectorField.alias, connectorField.columnType);
            typeMapFormColumnName.put(columnSelector, connectorField.columnType);
        }

        select = new Select(Operations.PROJECT, mapping, types, typeMapFormColumnName);

        return select;

    }

    class ConnectorField {
        public String name;
        public String alias;
        public ColumnType columnType;

        public ConnectorField(String name, String alias, ColumnType columnType) {
            this.name = name;
            this.alias = alias;
            this.columnType = columnType;
        }

    }
}
