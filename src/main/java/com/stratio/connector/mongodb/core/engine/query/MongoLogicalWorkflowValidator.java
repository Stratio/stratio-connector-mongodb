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
import com.stratio.connector.commons.engine.query.ProjectValidator;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;

/**
 * Validates the logical workflow and stores the needed steps.
 */
public class MongoLogicalWorkflowValidator implements ProjectValidator {

    /**
     * This method validate the projectParsed.
     * 
     * @param projectParsed
     *            the projectParsed.
     *
     * @throws ExecutionException
     *             if the project is not validated.
     */
    @Override
    public void validate(ProjectParsed projectParsed) throws MongoValidationException {

        if (projectParsed.getProject() == null) {
            throw new MongoValidationException("Projection has not been found in the logical workflow");
        }
        if (projectParsed.getSelect() == null) {
            throw new MongoValidationException("Select has not been found in the logical workflow");

        }
        if (!projectParsed.getMatchList().isEmpty()) {
            throw new MongoValidationException("Full-text queries not yet supported");
        }

        // do {
        // if (select != null) {
        // throw new MongoValidationException("Select must be the last step");
        // }
        //
        // if (logicalStep instanceof Project) {
        // if (projection == null) {
        // projection = (Project) logicalStep;
        // } else {
        // throw new MongoValidationException(" # Project > 1");
        // }
        // } else if (logicalStep instanceof Filter) {
        // Filter step = (Filter) logicalStep;
        // if (Operator.MATCH == step.getRelation().getOperator()) {
        // throw new MongoValidationException("Full-text queries not yet supported");
        // } else {
        // filterList.add(step);
        // }
        // } else if (logicalStep instanceof Limit) {
        // if (limit == null) {
        // limit = (Limit) logicalStep;
        // } else {
        // throw new MongoValidationException(" # Limit > 1");
        // }
        // } else if (logicalStep instanceof GroupBy) {
        // if (groupBy == null) {
        // groupBy = (GroupBy) logicalStep;
        // } else {
        // throw new MongoValidationException(" # GroupBy > 1");
        // }
        // } else if (logicalStep instanceof Select) {
        // select = (Select) logicalStep;
        // } else {
        // throw new MongoValidationException("Step unsupported" + logicalStep.getClass());
        // }
        // } while ((logicalStep = logicalStep.getNextStep()) != null);
        //
        // if (projection == null) {
        // throw new MongoValidationException("Projection has not been found in the logical workflow");
        // }
        // if (select == null) {
        // throw new MongoValidationException("Select has not been found in the logical workflow");
        // }
    }

}
