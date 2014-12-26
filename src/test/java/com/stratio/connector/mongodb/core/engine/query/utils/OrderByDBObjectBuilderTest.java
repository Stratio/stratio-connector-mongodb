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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;

public class OrderByDBObjectBuilderTest {

    private static String CATALOG = "catalog";
    private static String TABLE = "table";
    private static String COLUMN_PRIMARY = "column";
    private static String COLUMN_SECONDARY = "column_sec";

    @Test
    public void basicOrderByDBObjectBuilderTest() throws MongoValidationException, ExecutionException {

        OrderByDBObjectBuilder orderByDBObjectBuilder = new OrderByDBObjectBuilder(false, getOrderBy(COLUMN_PRIMARY));

        DBObject internalDBObject = (DBObject) Whitebox.getInternalState(orderByDBObjectBuilder, "orderByQuery");

        assertTrue("The orderBy query must have the column" + COLUMN_PRIMARY,
                        internalDBObject.containsField(COLUMN_PRIMARY));
        assertEquals("The orderBy value is not the expected", 1, internalDBObject.get(COLUMN_PRIMARY));
        assertEquals("The orderBy command is not the expected", new BasicDBObject(COLUMN_PRIMARY, 1), internalDBObject);

    }

    @Test
    public void combinedOrderByDBObjectBuilderTest() throws MongoValidationException, ExecutionException {

        OrderBy orderBy = getOrderBy(new String[] { COLUMN_PRIMARY, COLUMN_SECONDARY }, new OrderDirection[] {
                        OrderDirection.DESC, OrderDirection.ASC });
        OrderByDBObjectBuilder orderByDBObjectBuilder = new OrderByDBObjectBuilder(false, orderBy);

        DBObject internalDBObject = (DBObject) Whitebox.getInternalState(orderByDBObjectBuilder, "orderByQuery");

        assertTrue("The orderBy query must have the column" + COLUMN_PRIMARY,
                        internalDBObject.containsField(COLUMN_PRIMARY));
        assertTrue("The orderBy query must have the column" + COLUMN_SECONDARY,
                        internalDBObject.containsField(COLUMN_SECONDARY));

        BasicDBObject expectedDBObject = new BasicDBObject(COLUMN_PRIMARY, -1);
        expectedDBObject.append(COLUMN_SECONDARY, 1);

        assertEquals("The orderBy command is not the expected", expectedDBObject, internalDBObject);

    }

    @Test
    public void buildTest() throws Exception {

        OrderByDBObjectBuilder orderByDBObjectBuilder = new OrderByDBObjectBuilder(false, getOrderBy("mock"));

        DBObject fakeOrderBy = new BasicDBObject("fake", 5);

        // Aggregation build
        Whitebox.setInternalState(orderByDBObjectBuilder, "orderByQuery", fakeOrderBy);
        Whitebox.setInternalState(orderByDBObjectBuilder, "useAggregation", true);

        DBObject orderByCommand = orderByDBObjectBuilder.build();

        assertTrue("The orderBy command should have a root $sort", orderByCommand.keySet().contains("$sort"));
        assertEquals("The value is not the expected", fakeOrderBy, orderByCommand.get("$sort"));

        // Basic build
        Whitebox.setInternalState(orderByDBObjectBuilder, "orderByQuery", fakeOrderBy);
        Whitebox.setInternalState(orderByDBObjectBuilder, "useAggregation", false);

        orderByCommand = orderByDBObjectBuilder.build();

        assertFalse("The orderBy command should not have a root $sort", orderByCommand.keySet().contains("$sort"));
        assertEquals("The value is not the expected", fakeOrderBy, orderByCommand);

    }

    private OrderBy getOrderBy(String[] fields, OrderDirection[] direction) {
        List<OrderByClause> ids = new ArrayList<OrderByClause>();
        for (int i = 0; i < fields.length; i++) {
            ids.add(new OrderByClause(direction[i], new ColumnSelector(new ColumnName(CATALOG, TABLE, fields[i]))));
        }
        return new OrderBy(Operations.SELECT_GROUP_BY, ids);
    }

    private OrderBy getOrderBy(String... fields) {
        OrderDirection[] orderDirection = new OrderDirection[fields.length];
        for (int i = 0; i < fields.length; i++) {
            orderDirection[i] = OrderDirection.ASC;
        }
        return getOrderBy(fields, orderDirection);
    }
}
