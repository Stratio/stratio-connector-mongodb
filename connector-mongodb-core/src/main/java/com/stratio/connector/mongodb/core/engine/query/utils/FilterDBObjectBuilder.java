/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.mongodb.core.engine.query.utils;

import java.util.List;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.relationships.Operator;
import com.stratio.crossdata.common.statements.structures.relationships.Relation;
import com.stratio.crossdata.common.statements.structures.selectors.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.selectors.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.selectors.FloatingPointSelector;
import com.stratio.crossdata.common.statements.structures.selectors.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.SelectorType;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

public class FilterDBObjectBuilder extends DBObjectBuilder {

    private DBObject filterQuery = null;

    public FilterDBObjectBuilder(boolean useAggregation) {
        super(useAggregation);
    }

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
     * @param filterList
     * @throws MongoValidationException
     */
    public void addAll(List<Filter> filterList) throws MongoValidationException {

        if (filterList.size() > 1) {
            BasicBSONList filterExpressions = new BasicBSONList();
            for (Filter filter : filterList) {
                filterExpressions.add(getFilterQuery(filter));
            }
            filterQuery = new BasicDBObject("$and", filterExpressions);
        } else if (filterList.size() == 1) {
            filterQuery = getFilterQuery(filterList.get(0));
        } else {
            filterQuery = new BasicDBObject();
        }

    }

    private DBObject getFilterQuery(Filter filter) throws MongoValidationException {

        Relation relation = filter.getRelation();
        Operator operator = relation.getOperator();
        Selector rightSelector = relation.getRightTerm();

        String fieldName = getFieldName(relation.getLeftTerm());
        validateSelector(operator, rightSelector.getType());
        DBObject fieldQuery = new BasicDBObject(getMongoOperator(operator), getMongoRightTerm(rightSelector));
        return new BasicDBObject(fieldName, fieldQuery);

    }

    private String getFieldName(Selector selector) {
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        }
        return field;
    }

    private void validateSelector(Operator operator, SelectorType selType) throws MongoValidationException {
        if (operator == Operator.LIKE && selType != SelectorType.STRING) {
            throw new MongoValidationException("The selector type: " + selType.toString()
                            + " is not supported with operator " + operator.toString());

        }
    }

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
        case COLUMN: // TODO $where?
        case ASTERISK:
        case FUNCTION:
        default:
            throw new MongoValidationException("Not yet supported");

        }
        return value;

    }

    /**
     * @param operator
     * @return
     * @throws MongoValidationException
     */
    private String getMongoOperator(Operator operator) throws MongoValidationException {
        String mongoOperator = null;

        switch (operator) {

        case DISTINCT:
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
            new MongoValidationException("Waiting for Meta to implement between filters");
            break;
        case IN:
            new MongoValidationException("Waiting for Meta to implement in filters");
            break;
        case ADD:
        case ASSIGN:
        case DIVISION:
        case MULTIPLICATION:
        case SUBTRACT:
        case MATCH:
        default:
            throw new MongoValidationException("The operator: " + operator.toString() + " is not supported");

        }
        return mongoOperator;

    }

}