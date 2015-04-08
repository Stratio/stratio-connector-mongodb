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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

public class ProjectBuilderTest {

    public static final String COLUMN_1 = "column1";
    public static final String ALIAS_COLUMN_1 = "alias1";
    public static final String COLUMN_2 = "column2";
    public static final String ALIAS_COLUMN_2 = "alias2";
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";
    public static final String CLUSTER_NAME = "cluster";

    @Test
    public void projectDBObjectBuilderTest() throws ExecutionException {

        // Project without aggregation => expected the select columns
        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, new ColumnType(DataType.VARCHAR)));
        Select select = getSelect(columns);
        Project project = getProject(COLUMN_1, COLUMN_2);
        ProjectDBObjectBuilder projectBuilder = new ProjectDBObjectBuilder(false, project, select);
        DBObject expectedProject = new BasicDBObject(COLUMN_1, 1);
        expectedProject.put("_id", 0);
        DBObject internalProjectDBObject = (DBObject) Whitebox.getInternalState(projectBuilder, "projectQuery");

        Assert.assertEquals("The project is not the expected", expectedProject, internalProjectDBObject);

        // Project with aggregation => expected the project columns
        projectBuilder = new ProjectDBObjectBuilder(true, project, select);
        expectedProject = new BasicDBObject(COLUMN_1, 1);
        expectedProject.put(COLUMN_2, 1);
        expectedProject.put("_id", 0);
        internalProjectDBObject = (DBObject) Whitebox.getInternalState(projectBuilder, "projectQuery");

        Assert.assertEquals("The project is not the expected", expectedProject, internalProjectDBObject);

    }

    private Project getProject(String... columnName) {
        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.PROJECT);
        Project project = new Project(operations, new TableName(CATALOG, TABLE), new ClusterName(CLUSTER_NAME));
        for (String col : columnName) {
            project.addColumn(new ColumnName(CATALOG, TABLE, col));
        }

        return project;
    }

    @Test
    public void buildTest() throws Exception {
        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, new ColumnType(DataType.VARCHAR)));
        Select select = getSelect(columns);
        Project project = getProject(COLUMN_1);
        ProjectDBObjectBuilder projectBuilder = new ProjectDBObjectBuilder(false, project, select);
        DBObject fakeProject = new BasicDBObject(COLUMN_1, 1);

        // Default build
        Whitebox.setInternalState(projectBuilder, "projectQuery", fakeProject);
        Whitebox.setInternalState(projectBuilder, "useAggregation", false);

        DBObject projectCommand = projectBuilder.build();

        assertEquals("The value is not the expected", fakeProject, projectCommand);

        // Aggregation build
        Whitebox.setInternalState(projectBuilder, "useAggregation", true);

        projectCommand = projectBuilder.build();

        assertTrue("The project command should have a root $project", projectCommand.keySet().contains("$project"));
        assertEquals("The value is not the expected", fakeProject, projectCommand.get("$project"));

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

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_OPERATOR);

        select = new Select(operations, mapping, types, typeMapFormColumnName);

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
