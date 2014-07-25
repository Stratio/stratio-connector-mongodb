package com.stratio.connector.mongodb.core.engine.utils;


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.meta.Sort;


public class SortDBObjectBuilder extends DBObjectBuilder {
	

		private BasicDBObject sortQuery;// asc o desc, y varios sort posibles =>
		
		
		public SortDBObjectBuilder(boolean useAggregation){
			super(DBObjectType.ORDERBY,useAggregation);
			sortQuery = new BasicDBObject();
				
		}

		public void add(Sort sort) {
				
			int sortType =  (sort.getType() == Sort.ASC) ? 1 : -1;	
			sortQuery.put(sort.getField(), sortType);
			
		}
		
		public DBObject build(){
			
			
			
			DBObject container;
			if(useAggregation){
				container = new BasicDBObject(); 
				container.put("$sort", sortQuery);
			}else container = sortQuery;
			
			//log.debug(container.toString());
			
			return container;
				
		}
	}


