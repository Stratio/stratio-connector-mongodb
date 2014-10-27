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

package com.stratio.connector.mongodb.testutils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.QualifiedNames;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.crossdata.common.statements.structures.window.TimeUnit;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

/**
 * Created by jmgomez on 16/09/14.
 */
public class LogicalWorkFlowCreator {

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    private final ClusterName clusterName;
    public String table = this.getClass().getSimpleName();
    public String catalog = "catalog_functional_test";
    Select select;
    List<ColumnName> columns = new ArrayList<>();
    List<Filter> filters = new ArrayList<>();
    private Limit limit;
    private Window window;

    public LogicalWorkFlowCreator(String catalog, String table, ClusterName clusterName) {
        this.catalog = catalog;
        this.table = table;
        this.clusterName = clusterName;
    }

    public LogicalWorkflow getLogicalWorkflow() {

        List<LogicalStep> logiclaSteps = new ArrayList<>();

        Project project = new Project(Operations.PROJECT, new TableName(catalog, table), clusterName, columns);
        LogicalStep lastStep = project;
        for (Filter filter : filters) {
            lastStep.setNextStep(filter);
            lastStep = filter;
        }
        if (limit != null) {

            lastStep.setNextStep(limit);
            lastStep = limit;

        }
        if (window != null) {
            lastStep.setNextStep(window);
            lastStep = window;
        }
        if (select == null) {
            Map<ColumnName, String> selectColumn = new LinkedHashMap<>();
            Map<String, ColumnType> typeMap = new LinkedHashMap<>();
            Map<ColumnName, ColumnType> columnTypeMap = new LinkedHashMap<>();
            for (ColumnName columnName : project.getColumnList()) {
                selectColumn.put(new ColumnName(catalog, table, columnName.getName()), columnName.getName());
                typeMap.put(columnName.getQualifiedName(), ColumnType.VARCHAR);
                columnTypeMap.put(columnName, ColumnType.VARCHAR);
            }

            select = new Select(Operations.PROJECT, selectColumn, typeMap, null); // The select is mandatory. If it
                                                                                  // doesn't
            // exist we
            // create with all project's columns with varchar type.

        }
        lastStep.setNextStep(select);
        select.setPrevious(lastStep);

        logiclaSteps.add(project);

        return new LogicalWorkflow(logiclaSteps);

    }

    public LogicalWorkFlowCreator addDefaultColumns() {

        addColumnName(COLUMN_1);
        addColumnName(COLUMN_2);
        addColumnName(COLUMN_AGE);
        addColumnName(COLUMN_MONEY);

        return this;
    }

    public LogicalWorkFlowCreator addBetweenFilter(String columnName, Object leftTerm, Object rightTerm) {

        throw new RuntimeException("Not yet implemented");
        // return this;
    }

    public LogicalWorkFlowCreator addColumnName(String... columnName) {
        for (int i = 0; i < columnName.length; i++) {
            columns.add(new ColumnName(catalog, table, columnName[i]));
        }

        return this;
    }

    public LogicalWorkFlowCreator addEqualFilter(String columnName, Object value, Boolean indexed, boolean pk) {
        Selector columnSelector = new ColumnSelector(new ColumnName(catalog, table, columnName));

        Operations operation = Operations.FILTER_INDEXED_EQ;
        if (pk) {
            operation = Operations.FILTER_PK_EQ;
        } else if (indexed) {
            operation = Operations.FILTER_INDEXED_EQ;
        } else {
            operation = Operations.FILTER_NON_INDEXED_EQ;
        }
        filters.add(new Filter(operation, new Relation(columnSelector, Operator.EQ, returnSelector(value))));
        return this;

    }

    private Selector returnSelector(Object value) {
        Selector valueSelector = null;

        if (value instanceof String) {
            valueSelector = new StringSelector((String) value);
        } else if (value instanceof Integer) {
            valueSelector = new IntegerSelector((Integer) value);
        }
        if (value instanceof Boolean) {
            valueSelector = new BooleanSelector((Boolean) value);
        }
        return valueSelector;
    }

    public LogicalWorkFlowCreator addGreaterEqualFilter(String columnName, Object term, Boolean indexed, boolean pk) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GET,
                        returnSelector(term));

        if (pk) {
            filters.add(new Filter(Operations.FILTER_PK_GET, relation));
        } else if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GET, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GET, relation));

        }

        return this;

    }

    public LogicalWorkFlowCreator addGreaterFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.GT,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_GT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_GT, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addLowerEqualFilter(String columnName, Object term, Boolean indexed) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LET,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LET, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LET, relation));
        }

        return this;

    }

    public LogicalWorkFlowCreator addNLowerFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LT,
                        returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_LT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_LT, relation));
        }

        return this;
    }

    public LogicalWorkFlowCreator addDistinctFilter(String columnName, Object term, Boolean indexed) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),
                        Operator.DISTINCT, returnSelector(term));
        if (indexed) {
            filters.add(new Filter(Operations.FILTER_INDEXED_DISTINCT, relation));
        } else {
            filters.add(new Filter(Operations.FILTER_NON_INDEXED_DISTINCT, relation));
        }

        return this;
    }

    public LogicalWorkFlowCreator addMatchFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)),
                        Operator.MATCH, returnSelector(textToFind));

        filters.add(new Filter(Operations.FILTER_FULLTEXT, relation));

        return this;
    }

    public LogicalWorkFlowCreator addLikeFilter(String columnName, String textToFind) {

        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), Operator.LIKE,
                        returnSelector(textToFind));

        filters.add(new Filter(Operations.FILTER_FULLTEXT, relation));

        return this;
    }

    public LogicalWorkFlowCreator addSelect(LinkedList<ConnectorField> fields) {
        Map<ColumnName, String> mapping = new LinkedHashMap<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        Map<ColumnName, ColumnType> columnTypeMap = new LinkedHashMap<>();

        for (ConnectorField connectorField : fields) {
            mapping.put(new ColumnName(catalog, table, connectorField.name), connectorField.alias);
            types.put(QualifiedNames.getColumnQualifiedName(catalog, table, connectorField.name),
                            connectorField.columnType);
            columnTypeMap.put(new ColumnName(catalog, table, connectorField.name), connectorField.columnType);
        }

        select = new Select(Operations.PROJECT, mapping, types, columnTypeMap);

        return this;

    }

    public LogicalWorkFlowCreator addWindow(WindowType type, int limit) throws UnsupportedException {

        window = new Window(Operations.FILTER_FUNCTION_EQ, type);
        switch (type) {
        case NUM_ROWS:
            window.setNumRows(limit);
            break;
        case TEMPORAL:
            window.setTimeWindow(limit, TimeUnit.SECONDS);
            break;
        default:
            throw new UnsupportedException("Window " + type + " not supported");

        }
        return this;
    }

    public ConnectorField createConnectorField(String name, String alias, ColumnType columnType) {
        return new ConnectorField(name, alias, columnType);

    }

    public LogicalWorkFlowCreator addLimit(int limit) {
        this.limit = new Limit(Operations.SELECT_LIMIT, limit);
        return this;
    }

    public class ConnectorField {
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