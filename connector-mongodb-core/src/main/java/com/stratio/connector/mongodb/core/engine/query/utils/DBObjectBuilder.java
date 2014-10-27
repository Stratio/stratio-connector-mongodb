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

import com.mongodb.DBObject;

/**
 * A generic DBObject builder to use in Mongo queries.
 */
public abstract class DBObjectBuilder {

    /** Whether the object is built for the aggregation framework or not. */
    private final boolean useAggregation;

    /**
     * Instantiates a new DB object builder.
     *
     * @param useAggregation
     *            whether the query use the aggregation framework or not
     */
    public DBObjectBuilder(boolean useAggregation) {
        this.useAggregation = useAggregation;
    }

    /**
     * Checks if the object use the pipeline.
     *
     * @return true if the object use the aggregation framework
     */
    public final boolean useAggregationPipeline() {
        return useAggregation;
    }

    /**
     * Builds the object.
     *
     * @return the DB object
     */
    public abstract DBObject build();

}
