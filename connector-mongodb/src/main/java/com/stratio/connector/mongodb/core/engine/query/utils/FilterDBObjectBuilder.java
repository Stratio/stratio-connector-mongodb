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

import java.util.Collection;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FloatingPointSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * The Class FilterDBObjectBuilder.
 */
public class FilterDBObjectBuilder extends DBObjectBuilder {

    /** The filter query. */
    private DBObject filterQuery = null;

    /**
     * Instantiates a new filter builder.
     *
     * @param useAggregation
     *            whether the query use the aggregation framework or not
     * @param filterCollection
     *            the set of filters
     * @throws MongoValidationException
     *             if the query specified in the logical workflow is not supported
     * @throws UnsupportedException
     *             if the specified operation is not supported
     */
    public FilterDBObjectBuilder(boolean useAggregation, Collection<Filter> filterCollection)
                    throws MongoValidationException, UnsupportedException {

        super(useAggregation);

        if (filterCollection.size() > 1) {
            BasicBSONList filterExpressions = new BasicBSONList();
            for (Filter filter : filterCollection) {
                filterExpressions.add(getFilterQuery(filter));
            }
            filterQuery = new BasicDBObject("$and", filterExpressions);
        } else if (filterCollection.size() == 1) {
            filterQuery = getFilterQuery(filterCollection.iterator().next());
        } else {
            filterQuery = new BasicDBObject();
        }
    }

    /**
     * Builds the object. Insert a $match if the aggregation framework is used.
     *
     * @return the DB object
     */
    public DBObject build() {
        DBObject container;
        if (useAggregationPipeline()) {
            container = new BasicDBObject();
            container.put("$match", filterQuery);
        } else {
            container = filterQuery;
        }

        return container;

    }

    /**
     * Gets the filter query.
     *
     * @param filter
     *            the filter
     * @return the filter query
     * @throws MongoValidationException
     *             if the filter specified in the logical workflow is not supported
     * @throws UnsupportedException
     *             if the specified operation is not supported
     */
    private DBObject getFilterQuery(Filter filter) throws MongoValidationException, UnsupportedException {

        Relation relation = filter.getRelation();
        Operator operator = relation.getOperator();
        Selector rightSelector = relation.getRightTerm();

        String fieldName = getFieldName(relation.getLeftTerm());
        validateSelector(operator, rightSelector.getType());
        String mongoOperator = getMongoOperator(operator);

        if (mongoOperator.equals("$eq") && !useAggregationPipeline()) {
            return new BasicDBObject(fieldName, getMongoRightTerm(rightSelector));
        } else {
            DBObject fieldQuery = new BasicDBObject(mongoOperator, getMongoRightTerm(rightSelector));
            return new BasicDBObject(fieldName, fieldQuery);
        }

    }

    /**
     * Gets the field corresponding to the selector.
     *
     * @param selector
     *            the selector
     * @return the field name
     */
    private String getFieldName(Selector selector) {
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        } else if (selector instanceof StringSelector) {
            // TODO It should be removed
            StringSelector stringSelector = (StringSelector) selector;
            field = stringSelector.getValue();
        }
        return field;
    }

    /**
     * Validate selector.
     *
     * @param operator
     *            the operator
     * @param selectorType
     *            the selector type
     * @throws MongoValidationException
     *             if the operator is not supported by the specified selector type
     */
    private void validateSelector(Operator operator, SelectorType selectorType) throws MongoValidationException {
        if (operator == Operator.LIKE && selectorType != SelectorType.STRING) {
            throw new MongoValidationException("The selector type: " + selectorType.toString()
                            + " is not supported with operator " + operator.toString());

        }
    }

    /**
     * Gets the filter right term.
     *
     * @param rightSelector
     *            the right selector
     * @return the filter right term
     * @throws MongoValidationException
     *             if the selector is not supported
     */
    private Object getMongoRightTerm(Selector rightSelector) throws MongoValidationException {

        Object value;
        switch (rightSelector.getType()) {
        case BOOLEAN:
            value = ((BooleanSelector) rightSelector).getValue();
            break;
        case INTEGER:
            value = ((IntegerSelector) rightSelector).getValue();
            break;
        case STRING:
            value = ((StringSelector) rightSelector).getValue();
            break;
        case FLOATING_POINT:
            value = ((FloatingPointSelector) rightSelector).getValue();
            break;
        case RELATION:
        case COLUMN:
        case ASTERISK:
        case FUNCTION:
        default:
            throw new MongoValidationException("Right selector type not supported: " + rightSelector.getType());

        }
        return value;

    }

    /**
     * Gets the mongo operator.
     *
     * @param operator
     *            the operator
     * @return the mongo operator
     * @throws MongoValidationException
     *             if the operator is not supported
     * @throws UnsupportedException
     *             if the operator is not supported
     */
    private String getMongoOperator(Operator operator) throws MongoValidationException, UnsupportedException {
        String mongoOperator;

        switch (operator) {

        case NOT_EQ:
            mongoOperator = "$ne";
            break;
        case GET:
            mongoOperator = "$gte";
            break;
        case GT:
            mongoOperator = "$gt";
            break;
        case LET:
            mongoOperator = "$lte";
            break;
        case LT:
            mongoOperator = "$lt";
            break;
        case EQ:
            mongoOperator = "$eq";
            break;
        case LIKE:
            mongoOperator = "$regex";
            break;
        case BETWEEN:
        case IN:
        case ADD:
        case ASSIGN:
        case DIVISION:
        case MULTIPLICATION:
        case SUBTRACT:
        case MATCH:
        default:
            throw new UnsupportedException("The operator: " + operator.toString() + " is not supported");
        }
        return mongoOperator;

    }

}