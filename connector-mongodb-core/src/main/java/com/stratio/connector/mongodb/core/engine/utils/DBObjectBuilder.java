package com.stratio.connector.mongodb.core.engine.utils;

import com.mongodb.DBObject;

public abstract class DBObjectBuilder {

	final private DBObjectType type;
	final public boolean useAggregation;
	
	public DBObjectBuilder(DBObjectType type, boolean useAggregation){
		this.type = type;
		this.useAggregation = useAggregation;
	}
	
	public DBObjectType getType(){
		return type;
	}
	public boolean useAggregationPipeline(){
		return useAggregation;
	}
	
	public abstract DBObject build();
	
}
