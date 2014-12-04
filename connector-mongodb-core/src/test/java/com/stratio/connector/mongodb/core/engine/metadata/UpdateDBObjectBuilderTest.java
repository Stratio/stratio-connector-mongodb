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

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class UpdateDBObjectBuilderTest {

    private static final String COLLECTION_NAME = "coll_name";
    private static final String DB_NAME = "db_name";
    private static final String COLUMN_NAME = "row_name";
    private static final String OTHER_COLUMN_NAME = "other_row_name";
    private static final String STRING_VALUE = "cell_value";
    private static final String STRING_VALUE_OTHER = "other_cell_value";
    private static final Long LONG_VALUE = 25l;

    @Test
    public void addUpdateRelationtest() throws UnsupportedException, ExecutionException {

        UpdateDBObjectBuilder updateDBObjectBuilder = new UpdateDBObjectBuilder();
        Assert.assertEquals("The update query should be empty", new BasicDBObject(), updateDBObjectBuilder.build());

        Relation rel = getBasicRelation(COLUMN_NAME, Operator.ASSIGN, STRING_VALUE);
        Relation returned = updateDBObjectBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(),
                        rel.getRightTerm());
        Assert.assertNull("The relation returned should be null", returned);
        BasicDBObject updatedExpected = new BasicDBObject();
        updatedExpected.append("$set", new BasicDBObject(COLUMN_NAME, STRING_VALUE));
        Assert.assertEquals("The update query is not the expected", updatedExpected, updateDBObjectBuilder.build());

        // The following operation should not modify the query
        returned = updateDBObjectBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(), rel.getRightTerm());
        Assert.assertEquals("The update query is not the expected", updatedExpected, updateDBObjectBuilder.build());
        Assert.assertNull("The relation returned should be null", returned);

        // The following operation should modify the set
        returned = updateDBObjectBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(), new StringSelector(
                        STRING_VALUE_OTHER));
        updatedExpected.append("$set", new BasicDBObject(COLUMN_NAME, STRING_VALUE_OTHER));
        Assert.assertEquals("The update query is not the expected", updatedExpected, updateDBObjectBuilder.build());
        Assert.assertNull("The relation returned should be null", returned);

        Relation rel2 = getBasicRelation(OTHER_COLUMN_NAME, Operator.SUBTRACT, 20l);
        returned = updateDBObjectBuilder.addUpdateRelation(rel2.getLeftTerm(), rel2.getOperator(), rel2.getRightTerm());
        updatedExpected.append("$inc", new BasicDBObject(OTHER_COLUMN_NAME, -20l));
        Assert.assertEquals("The update query is not the expected", updatedExpected, updateDBObjectBuilder.build());
        Assert.assertNull("The relation returned should be null", returned);
    }

    /**
     * Method under test addUpdateRelation().
     *
     * @throws UnsupportedException
     *             the unsupported exception
     * @throws ExecutionException
     *             the execution exception
     */
    @Test
    public void addUpdateInnerRelationTest() throws UnsupportedException, ExecutionException {

        UpdateDBObjectBuilder updateDBObjectBuilder = new UpdateDBObjectBuilder();

        Relation rel = getBasicRelation(OTHER_COLUMN_NAME, Operator.ASSIGN,
                        getBasicRelation(OTHER_COLUMN_NAME, Operator.SUBTRACT, 20l));

        Relation returned = updateDBObjectBuilder.addUpdateRelation(rel.getLeftTerm(), rel.getOperator(),
                        rel.getRightTerm());

        Assert.assertEquals("The inner relations returned is not the expected",
                        getBasicRelation(OTHER_COLUMN_NAME, Operator.SUBTRACT, 20l).toString(), returned.toString());

    }

    private Relation getBasicRelation(String column1, Operator assign, Object valueUpdated) {
        Selector leftSelector = new ColumnSelector(new ColumnName(DB_NAME, COLLECTION_NAME, column1));
        leftSelector.setAlias(column1);
        Selector rightSelector = null;
        if (valueUpdated instanceof Integer) {
            rightSelector = new IntegerSelector((int) valueUpdated);
        } else if (valueUpdated instanceof String) {
            rightSelector = new StringSelector((String) valueUpdated);
        } else if (valueUpdated instanceof Boolean) {
            rightSelector = new BooleanSelector((Boolean) valueUpdated);
        } else if (valueUpdated instanceof Relation) {
            rightSelector = new RelationSelector((Relation) valueUpdated);
        } else if (valueUpdated instanceof Long) {
            rightSelector = new IntegerSelector((int) (long) valueUpdated);
        }
        return new Relation(leftSelector, assign, rightSelector);
    }

}
