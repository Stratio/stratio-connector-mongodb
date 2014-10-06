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
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta2.common.data.ColumnName;

public class ProjectDBObjectBuilder extends DBObjectBuilder {

    private BasicDBObject projectQuery;

    public ProjectDBObjectBuilder(boolean useAggregation, Select select) throws MongoValidationException {
        super(useAggregation);

        projectQuery = new BasicDBObject();
        Set<ColumnName> columnMetadataList = select.getColumnMap().keySet();

        if (columnMetadataList == null || columnMetadataList.isEmpty()) {
            throw new MongoValidationException("The query has to request at least one field");
        } else {

            for (ColumnName columnName : columnMetadataList) {
                projectQuery.put(columnName.getName(), 1);
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