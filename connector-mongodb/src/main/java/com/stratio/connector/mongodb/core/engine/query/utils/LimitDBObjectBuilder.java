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
import com.stratio.crossdata.common.logicalplan.Limit;

/**
 * The Class LimitDBObjectBuilder.
 */
public class LimitDBObjectBuilder extends DBObjectBuilder {

    /** The limit. */
    private int limit;

    /**
     * Instantiates a new limit builder.
     *
     * @param limit
     *            the limit
     */
    public LimitDBObjectBuilder(Limit limit) {
        super(true);
        this.limit = limit.getLimit();
    }

    /**
     * Builds the object. Insert a $limit if the aggregation framework is used.
     *
     * @return the DB object
     */
    @Override
    public DBObject build() {
        return new BasicDBObject("$limit", limit);
    }

}
