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
package com.stratio.connector.mongodb.core.engine.query;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;

/**
 * A factory for creating LogicalWorkflowExecutor objects.
 */
public class LogicalWorkflowExecutorFactory {

    /**
     * Checks Whether the aggregation framework is required for logicalworkflow or not.
     *
     * @param logicalWorkfloParsed
     *            the logical workflow parsed
     * @throws MongoValidationException
     *             if the query specified in the logical workflow is not supported.
     * @throws ExecutionException
     *             if the execution fails.
     *
     * @return a BasicLogicalWorkflowExecutor if the aggregation is not required. Otherwise, a
     *         AggregationLogicalWorkflowExecutor is returned.
     */
    public static LogicalWorkflowExecutor getLogicalWorkflowExecutor(ProjectParsed logicalWorkfloParsed)
                    throws MongoValidationException, ExecutionException {
        LogicalWorkflowExecutor logicalWorkflowExecutor;

        // Computes if the aggregation is required.

        if (logicalWorkfloParsed.getGroupBy() != null) {
            logicalWorkflowExecutor = new AggregationLogicalWorkflowExecutor(logicalWorkfloParsed);
        } else {
            logicalWorkflowExecutor = new BasicLogicalWorkflowExecutor(logicalWorkfloParsed);
        }
        return logicalWorkflowExecutor;

    }
}
