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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.bson.types.BasicBSONList;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * @author david
 */
public class FilterDBObjectBuilderTest {

    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    @Test
    public void addSimpleFilterTest() throws Exception {

        Filter filter = buildFilter(Operations.FILTER_NON_INDEXED_GET, CATALOG, TABLE, COLUMN_1, Operator.GET, 5);

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(false, Arrays.asList(filter));
        assertFalse((Boolean) Whitebox.getInternalState(filterDBObjectBuilder, "useAggregation"));

        DBObject filterQuery = (DBObject) Whitebox.getInternalState(filterDBObjectBuilder, "filterQuery");
        assertNotNull(filterQuery);
        assertEquals(1, filterQuery.keySet().size());
        assertTrue(filterQuery.containsField(COLUMN_1));
        DBObject condition = (DBObject) filterQuery.get(COLUMN_1);
        assertTrue(condition.containsField("$gte"));
        assertEquals(5l, condition.get("$gte"));

    }

    @Test
    public void addMultipleFiltersTest() throws Exception {

        Filter filtergte = buildFilter(Operations.FILTER_NON_INDEXED_GET, CATALOG, TABLE, COLUMN_1, Operator.GET, 5);
        Filter filterdi = buildFilter(Operations.FILTER_NON_INDEXED_DISTINCT, CATALOG, TABLE, COLUMN_1,
                        Operator.DISTINCT, "five");
        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(false, Arrays.asList(filtergte,
                        filterdi));
        assertFalse((Boolean) Whitebox.getInternalState(filterDBObjectBuilder, "useAggregation"));

        DBObject filterQuery = (DBObject) Whitebox.getInternalState(filterDBObjectBuilder, "filterQuery");
        assertNotNull(filterQuery);
        assertEquals(1, filterQuery.keySet().size());
        assertTrue(filterQuery.containsField("$and"));
        BasicBSONList filters = (BasicBSONList) filterQuery.get("$and");

        DBObject dbFilters = (DBObject) filters.get(0);
        DBObject condition = (DBObject) dbFilters.get(COLUMN_1);
        assertTrue(condition.containsField("$gte"));
        assertEquals(5l, condition.get("$gte"));

        DBObject dbFilters2 = (DBObject) filters.get(1);
        DBObject condition2 = (DBObject) dbFilters2.get(COLUMN_1);
        assertTrue(condition2.containsField("$ne"));
        assertEquals("five", condition2.get("$ne"));

    }

    @Test(expected = MongoValidationException.class)
    public void addNotSupportedFilterTest() throws Exception {

        Filter filter = buildFilter(Operations.FILTER_NON_INDEXED_GET, CATALOG, TABLE, COLUMN_1, Operator.MATCH, 5);

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(false, Arrays.asList(filter));
        assertFalse((Boolean) Whitebox.getInternalState(filterDBObjectBuilder, "useAggregation"));

    }

    // TODO add $regex and floatingpoint test

    @Test
    public void buildTest() throws Exception {
        @SuppressWarnings("unchecked")
        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(false, Collections.EMPTY_LIST);

        DBObject dbObject = QueryBuilder.start(COLUMN_1).greaterThanEquals(5).get();

        Whitebox.setInternalState(filterDBObjectBuilder, "filterQuery", dbObject);
        Whitebox.setInternalState(filterDBObjectBuilder, "useAggregation", false);

        DBObject filterQuery = filterDBObjectBuilder.build();

        assertNotNull(filterQuery);
        assertEquals(1, filterQuery.keySet().size());
        assertTrue(filterQuery.containsField(COLUMN_1));
        DBObject condition = (DBObject) filterQuery.get(COLUMN_1);
        assertTrue(condition.containsField("$gte"));
        assertEquals(5, condition.get("$gte"));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void buildAggregationTest() throws Exception {
        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(true, Collections.EMPTY_LIST);

        DBObject dbObject = QueryBuilder.start(COLUMN_1).greaterThanEquals(5).get();

        Whitebox.setInternalState(filterDBObjectBuilder, "filterQuery", dbObject);
        Whitebox.setInternalState(filterDBObjectBuilder, "useAggregation", true);

        DBObject filterQuery = filterDBObjectBuilder.build();

        assertNotNull(filterQuery);
        assertEquals(1, filterQuery.keySet().size());
        assertTrue(filterQuery.containsField("$match"));
        DBObject condition = (DBObject) ((DBObject) filterQuery.get("$match")).get(COLUMN_1);
        assertTrue(condition.containsField("$gte"));
        assertEquals(5, condition.get("$gte"));

    }

    private Filter buildFilter(Operations operation, String catalog, String table, String columnName,
                    Operator operator, Object value) {
        Relation relation = new Relation(new ColumnSelector(new ColumnName(catalog, table, columnName)), operator,
                        returnSelector(value));
        return new Filter(operation, relation);
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
}
