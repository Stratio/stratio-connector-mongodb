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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.crossdata.common.statements.structures.SelectorType;

/**
 * The Class OrderByDBObjectBuilder. Allows build a "$sort" MongoDB query.
 */
public class OrderByDBObjectBuilder extends DBObjectBuilder {

    /** The orderBy query. */
    private BasicDBObject orderByQuery;

    /**
     * Instantiates a new order by DBObject builder.
     *
     * @param useAggregation
     *            whether the query use the aggregation framework or not
     * @param orderBy
     *            the orderBy conditions
     * @throws ExecutionException
     *             if the conditions specified in the logical workflow are not supported
     */
    public OrderByDBObjectBuilder(boolean useAggregation, OrderBy orderBy) throws ExecutionException {
        super(useAggregation);
        orderByQuery = new BasicDBObject();

        for (OrderByClause orderClause : orderBy.getIds()) {
            add(orderClause);
        }

    }

    private void add(OrderByClause orderClause) throws ExecutionException {
        String column = (String) SelectorHelper.getRestrictedValue(orderClause.getSelector(), SelectorType.COLUMN);
        int sortType = (orderClause.getDirection() == OrderDirection.ASC) ? 1 : -1;
        orderByQuery.put(column, sortType);
    }

    /**
     * Builds the DBObject. Insert a $sort if the aggregation framework is used.
     *
     * @return the DB object
     */
    public DBObject build() {

        DBObject container;
        if (useAggregationPipeline()) {
            container = new BasicDBObject();
            container.put("$sort", orderByQuery);
        } else {
            container = orderByQuery;
        }

        return container;

    }
}
