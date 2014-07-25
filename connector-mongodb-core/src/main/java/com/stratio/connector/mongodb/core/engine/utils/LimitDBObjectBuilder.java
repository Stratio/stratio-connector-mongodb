package com.stratio.connector.mongodb.core.engine.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.meta.Limit;

public class LimitDBObjectBuilder extends DBObjectBuilder {

	int limit;
	
	public LimitDBObjectBuilder(Limit limit) {
		super(DBObjectType.LIMIT, false); //only with aggregationFramework
		this.limit = limit.getLimit();
	}

	

	@Override
	public DBObject build() {
		return new BasicDBObject("$limit",limit);
	}

}
