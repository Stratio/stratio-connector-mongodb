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

/**
 * @author david
 *
 */

import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;


public class GroupByDBObjectBuilder extends DBObjectBuilder {

    private BasicDBObject groupQuery;

    public GroupByDBObjectBuilder(GroupBy group, Set<ColumnName> set) throws ExecutionException {
        super(true);
        groupQuery = new BasicDBObject();
        
        String identifier;
        List<Selector> ids = group.getIds();
        

        if (ids.size() == 1) {
            identifier = (String) SelectorHelper.getRestrictedValue(ids.get(0),SelectorType.COLUMN);
            groupQuery.put("_id", "$" + identifier);
            //TODO check $identifier or if it is necessary a custom one.
        } else if(ids.size() > 1) {
            BasicDBObject groupFields = new BasicDBObject();
            for (Selector selector: ids) {
                identifier = (String) SelectorHelper.getRestrictedValue(selector,SelectorType.COLUMN);
                groupFields .put(identifier, "$" + identifier);
            }
            groupQuery.put("_id", groupFields);// si hay uno => solo incluir un campo
        }
        for(ColumnName colName: set){
            groupQuery.put(colName.getName(), new BasicDBObject("$first","$"+colName.getName()));
        }
        

    }

    public DBObject build() { 
        return new BasicDBObject("$group", groupQuery);
    }
}