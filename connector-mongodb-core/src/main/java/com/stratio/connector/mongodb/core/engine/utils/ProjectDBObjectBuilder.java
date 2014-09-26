/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.mongodb.core.engine.utils;

import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;

public class ProjectDBObjectBuilder extends DBObjectBuilder {

    private BasicDBObject projectQuery;

    public ProjectDBObjectBuilder(boolean useAggregation, Project projection, Select select) throws MongoQueryException {
        super(/* DBObjectType.PROJECT, */useAggregation);

        projectQuery = new BasicDBObject();
        Set<String> columnMetadataList = select.getColumnMap().keySet();

        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            throw new MongoQueryException("The query has to retrieve data");
        } else {

            for (String columnName : columnMetadataList) {
                String[] splitColumnName = columnName.split("\\.");
                projectQuery.put(splitColumnName[splitColumnName.length - 1], 1);
            }

        }

    }

    @Override
    public DBObject build() {
        DBObject projectDBObject;

        if (useAggregation) {
            projectDBObject = new BasicDBObject("$project", projectQuery);
        } else
            projectDBObject = projectQuery;

        return projectDBObject;
    }
}