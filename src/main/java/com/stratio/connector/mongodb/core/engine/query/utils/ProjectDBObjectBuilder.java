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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * The Class ProjectDBObjectBuilder.
 */
public class ProjectDBObjectBuilder extends DBObjectBuilder {

    /** The fields projected in Mongo. */
    private DBObject projectQuery;

    /**
     * Instantiates a new project builder.
     *
     * @param useAggregation
     *            whether the project use the aggregation framework or not
     * @param project
     *            the project
     * @param select
     *            the select
     * @throws ExecutionException
     *             if the select specified in the logical workflow is not supported
     */
    public ProjectDBObjectBuilder(boolean useAggregation, Project project, Select select) throws ExecutionException {
        super(useAggregation);

        projectQuery = new BasicDBObject();
        List<ColumnName> columnNameList;

        if (!useAggregation) {
            Set<Selector> columnSelectorList = select.getColumnMap().keySet();
            columnNameList = new ArrayList<ColumnName>();
            for (Selector selector : columnSelectorList) {
                columnNameList.add(((ColumnSelector) selector).getColumnName());
            }

        } else {
            columnNameList = project.getColumnList();
        }

        if (columnNameList == null || columnNameList.isEmpty()) {
            throw new MongoValidationException("The query has to request at least one field");
        } else {

            for (ColumnName columnName : columnNameList) {
                projectQuery.put(columnName.getName(), 1);
            }
            projectQuery.put("_id", 0);

        }

    }

    /**
     * Builds the object. Insert a $project if the aggregation framework is used.
     *
     * @return the DB object
     */
    @Override
    public DBObject build() {
        DBObject projectDBObject;

        if (useAggregationPipeline()) {
            projectDBObject = new BasicDBObject("$project", projectQuery);
        } else {
            projectDBObject = projectQuery;
        }

        return projectDBObject;
    }
}