package com.stratio.connector.mongodb.core.engine.utils;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;

public class ProjectDBObjectBuilder extends DBObjectBuilder {

	private BasicDBObject projectQuery;
	

	
	
	public ProjectDBObjectBuilder(boolean useAggregation, Project projection){
		super(DBObjectType.PROJECT,useAggregation);
		
		projectQuery = new BasicDBObject();
		List<ColumnMetadata> columnMetadata = projection.getColumnList();
		
		if(columnMetadata == null || columnMetadata.isEmpty() ) {
    		//throw new ValidationException? select *
    		System.out.println("ValidException");
    	}else{
    		
			for(ColumnMetadata colMetadata: columnMetadata){
				projectQuery.put(colMetadata.getColumnName(), 1);//no comprobar columName...?
			}
			
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