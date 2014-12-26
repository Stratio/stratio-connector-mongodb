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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

public class GroupByDBObjectBuilderTest {

    private static String CATALOG = "catalog";
    private static String TABLE = "table";
    private static String GROUP_COLUMN = "column";
    private static String GROUP_COLUMN_SECONDARY = "column_sec";
    private static String SELECTED_FIELD = "sel_column";

    @Test
    public void groupByDBObjectBuilderTest() throws MongoValidationException, ExecutionException {

        // GroupBy with one column
        Set<ColumnName> colSelected = new HashSet<ColumnName>(Arrays.asList(new ColumnName(CATALOG, TABLE,
                        SELECTED_FIELD)));
        GroupBy groupBy = getGroupBy(GROUP_COLUMN);
        GroupByDBObjectBuilder groupByDBObjectBuilder = new GroupByDBObjectBuilder(groupBy, colSelected);
        assertTrue("useAggregations should be true",
                        (Boolean) Whitebox.getInternalState(groupByDBObjectBuilder, "useAggregation"));

        DBObject aggregationOperation = new BasicDBObject("$first", "$" + SELECTED_FIELD);

        DBObject internalProjectDBObject = (DBObject) Whitebox.getInternalState(groupByDBObjectBuilder, "groupQuery");

        Assert.assertEquals("The groupby command is not the expected", "$" + GROUP_COLUMN,
                        internalProjectDBObject.get("_id"));
        Assert.assertEquals("The groupby operand is not the expected", aggregationOperation,
                        internalProjectDBObject.get(SELECTED_FIELD));

        // GroupBy with several columns
        groupBy = getGroupBy(GROUP_COLUMN, GROUP_COLUMN_SECONDARY);
        groupByDBObjectBuilder = new GroupByDBObjectBuilder(groupBy, colSelected);

        groupByDBObjectBuilder = new GroupByDBObjectBuilder(groupBy, colSelected);
        assertTrue("useAggregations should be true",
                        (Boolean) Whitebox.getInternalState(groupByDBObjectBuilder, "useAggregation"));

        aggregationOperation = new BasicDBObject("$first", "$" + SELECTED_FIELD);
        DBObject groupColumnsFields = new BasicDBObject(GROUP_COLUMN, "$" + GROUP_COLUMN);
        groupColumnsFields.put(GROUP_COLUMN_SECONDARY, "$" + GROUP_COLUMN_SECONDARY);
        internalProjectDBObject = (DBObject) Whitebox.getInternalState(groupByDBObjectBuilder, "groupQuery");

        Assert.assertEquals("The groupby command is not the expected", groupColumnsFields,
                        internalProjectDBObject.get("_id"));
        Assert.assertEquals("The groupby operand is not the expected", aggregationOperation,
                        internalProjectDBObject.get(SELECTED_FIELD));

    }

    @Test
    public void buildTest() throws Exception {

        Set<ColumnName> colSelected = new HashSet<ColumnName>(Arrays.asList(new ColumnName(CATALOG, TABLE,
                        SELECTED_FIELD)));
        GroupBy groupBy = getGroupBy(GROUP_COLUMN);
        GroupByDBObjectBuilder groupByDBObjectBuilder = new GroupByDBObjectBuilder(groupBy, colSelected);

        DBObject fakeGroupBy = new BasicDBObject("fake", 5);

        // Default build
        Whitebox.setInternalState(groupByDBObjectBuilder, "groupQuery", fakeGroupBy);
        Whitebox.setInternalState(groupByDBObjectBuilder, "useAggregation", true);

        DBObject groupByCommand = groupByDBObjectBuilder.build();

        assertTrue("The groupBy command should have a root $group", groupByCommand.keySet().contains("$group"));
        assertEquals("The value is not the expected", fakeGroupBy, groupByCommand.get("$group"));

    }

    public GroupBy getGroupBy(String... fields) {
        List<Selector> ids = new ArrayList<Selector>();
        for (String field : fields) {
            ids.add(new ColumnSelector(new ColumnName(CATALOG, TABLE, field)));
        }
        return new GroupBy(Operations.SELECT_GROUP_BY, ids);
    }
}
