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

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta2.common.data.ColumnName;

public class ProjectDBObjectBuilder extends DBObjectBuilder {

	private BasicDBObject projectQuery;
	

	
	
	public ProjectDBObjectBuilder(boolean useAggregation, Project projection){
		super(/*DBObjectType.PROJECT,*/useAggregation);
		
		projectQuery = new BasicDBObject();
		List<ColumnName> columnMetadata = projection.getColumnList();
		if(columnMetadata == null || columnMetadata.isEmpty() ) {
    		//throw new ValidationException? select *
    	}else{
    		
			for(ColumnName colMetadata: columnMetadata){
				projectQuery.put(colMetadata.getName(), 1);//no comprobar columName...?
			}
			
			//TODO como va a llegar la PK, trasnformar a _id?
		if(!projectQuery.containsField("_id")) projectQuery.put("_id", 0); //seleccionar con boolean?
		}
		
			
	}


	@Override
	public DBObject build() {
		DBObject projectDBObject;
		
		if(useAggregation){
			projectDBObject = new BasicDBObject("$project",projectQuery);
		}else projectDBObject = projectQuery;
		
		return projectDBObject;
	}
}