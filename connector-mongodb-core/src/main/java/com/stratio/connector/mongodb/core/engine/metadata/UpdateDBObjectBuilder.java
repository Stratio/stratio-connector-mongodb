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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;

/**
 * The Class UpdateDBObjectBuilder. Allows build a DBObject update query.
 */
public class UpdateDBObjectBuilder {

    /** The logger. */
    private final static Logger logger = LoggerFactory.getLogger(UpdateDBObjectBuilder.class);

    /** The Constant INCREMENT_COMMAND. */
    private static final String INCREMENT_COMMAND = "$inc";
    /** The Constant MUTLTIPLICATION_COMMAND. */
    private static final String MUTLTIPLICATION_COMMAND = "$mul";
    /** The Constant SET_COMMAND. */
    private static final String SET_COMMAND = "$set";
    /** The relations. */
    private BasicDBObject relations;

    /**
     * Instantiates a new update DBObject builder.
     */
    public UpdateDBObjectBuilder() {
        relations = new BasicDBObject();
    }

    /**
     * Adds an update relation.
     *
     * @param left
     *            the left selector
     * @param operator
     *            the operator
     * @param right
     *            the right selector
     * @return a new relation which is needed to manage. Otherwise, a null value is returned
     * @throws UnsupportedException
     *             the unsupported exception
     * @throws ExecutionException
     *             the execution exception
     */
    public Relation addUpdateRelation(Selector left, Operator operator, Selector right) throws UnsupportedException,
                    ExecutionException {
        BasicDBObject basicDBObject;
        Relation relation = null;

        switch (operator) {

        case EQ:
        case ASSIGN:
            if (containsAnInnerRelation(left, right)) {
                return ((RelationSelector) right).getRelation();
            } else {
                if (relations.containsField(SET_COMMAND)) {
                    basicDBObject = (BasicDBObject) relations.get(SET_COMMAND);
                    basicDBObject.putAll(getBasicRelation(left, right));
                } else {
                    basicDBObject = new BasicDBObject();
                    basicDBObject.putAll(getBasicRelation(left, right));
                    relations.put(SET_COMMAND, basicDBObject);
                }
            }
            break;

        case ADD:
            if (relations.containsField(INCREMENT_COMMAND)) {
                basicDBObject = (BasicDBObject) relations.get(INCREMENT_COMMAND);
                basicDBObject.putAll(getIncrementalRelation(left, right, false));
            } else {
                basicDBObject = new BasicDBObject();
                basicDBObject.putAll(getIncrementalRelation(left, right, false));
                relations.put(INCREMENT_COMMAND, basicDBObject);
            }
            break;

        case SUBTRACT:
            if (relations.containsField(INCREMENT_COMMAND)) {
                basicDBObject = (BasicDBObject) relations.get(INCREMENT_COMMAND);
                basicDBObject.putAll(getIncrementalRelation(left, right, true));
            } else {
                basicDBObject = new BasicDBObject();
                basicDBObject.putAll(getIncrementalRelation(left, right, true));
                relations.put(INCREMENT_COMMAND, basicDBObject);
            }
            break;

        case MULTIPLICATION:
            if (relations.containsField(MUTLTIPLICATION_COMMAND)) {
                basicDBObject = (BasicDBObject) relations.get(MUTLTIPLICATION_COMMAND);
                basicDBObject.putAll(getNumberRelation(left, right));
            } else {
                basicDBObject = new BasicDBObject();
                basicDBObject.putAll(getNumberRelation(left, right));
                relations.put(MUTLTIPLICATION_COMMAND, basicDBObject);
            }
            break;

        default:
            String msg = "Operator: " + operator + " is not supported in update queries";
            logger.error(msg);
            throw new UnsupportedException(msg);
        }

        return relation;
    }

    private boolean containsAnInnerRelation(Selector left, Selector right) throws UnsupportedException,
                    ExecutionException {

        if (right.getType() == SelectorType.RELATION) {
            String column = (String) SelectorHelper.getRestrictedValue(left, SelectorType.COLUMN);
            RelationSelector rightSelector = (RelationSelector) right;
            String innerRelationColumn = (String) SelectorHelper.getRestrictedValue(rightSelector.getRelation()
                            .getLeftTerm(), SelectorType.COLUMN);
            if (column.equals(innerRelationColumn)) {
                return true;
            } else {
                String msg = "Update relations only can envolve a single field, but found:" + column + " and "
                                + innerRelationColumn;
                logger.error(msg);
                throw new UnsupportedException(msg);
            }

        }
        return false;
    }

    /**
     * Builds the update DBObject.
     *
     * @return the DB object
     */
    public DBObject build() {
        return relations;
    }

    private DBObject getNumberRelation(Selector left, Selector right) throws MongoValidationException,
                    ExecutionException {
        Number number = retrieveNumberDataType(right, false);
        return new BasicDBObject((String) SelectorHelper.getRestrictedValue(left, SelectorType.COLUMN), number);
    }

    private DBObject getBasicRelation(Selector left, Selector right) throws ExecutionException {
        return new BasicDBObject((String) SelectorHelper.getRestrictedValue(left, SelectorType.COLUMN),
                        SelectorHelper.getValue(right));

    }

    private DBObject getIncrementalRelation(Selector left, Selector right, boolean isDecrement)
                    throws ExecutionException, MongoValidationException {

        Number number = retrieveNumberDataType(right, isDecrement);
        return new BasicDBObject((String) SelectorHelper.getRestrictedValue(left, SelectorType.COLUMN), number);

    }

    /**
     * Checks if the column type is supported in the keys.
     *
     * @param columnType
     *            the columnType type
     * @param cellValue
     *            the cell value
     * @throws MongoValidationException
     *             if the type is not supported
     * @throws ExecutionException
     */
    private static Number retrieveNumberDataType(Selector selector, boolean isDecrement)
                    throws MongoValidationException, ExecutionException {
        Number number = null;
        switch (selector.getType()) {
        case FLOATING_POINT:
            double dValue = (double) SelectorHelper.getRestrictedValue(selector, SelectorType.FLOATING_POINT);
            number = (isDecrement) ? -dValue : dValue;
            break;
        case INTEGER:
            long iValue = (long) SelectorHelper.getRestrictedValue(selector, SelectorType.INTEGER);
            number = (isDecrement) ? -iValue : iValue;
            break;
        default:
            String msg = "The requested operation does not support the type: " + selector.getType().toString();
            logger.error(msg);
            throw new MongoValidationException(msg);

        }
        return number;

    }
}
