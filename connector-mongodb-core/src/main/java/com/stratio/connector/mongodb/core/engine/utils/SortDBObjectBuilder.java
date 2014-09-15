///**
//* Copyright (C) 2014 Stratio (http://stratio.com)
//*
//* Licensed under the Apache License, Version 2.0 (the "License");
//* you may not use this file except in compliance with the License.
//* You may obtain a copy of the License at
//*
//* http://www.apache.org/licenses/LICENSE-2.0
//*
//* Unless required by applicable law or agreed to in writing, software
//* distributed under the License is distributed on an "AS IS" BASIS,
//* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//* See the License for the specific language governing permissions and
//* limitations under the License.
//*/
//package com.stratio.connector.mongodb.core.engine.utils;
//
//
//import com.mongodb.BasicDBObject;
//import com.mongodb.DBObject;
//import com.mongodb.QueryBuilder;
//
//
//public class SortDBObjectBuilder extends DBObjectBuilder {
//	
//
//		private BasicDBObject sortQuery;// asc o desc, y varios sort posibles =>
//		
//		
//		public SortDBObjectBuilder(boolean useAggregation){
//			//super(DBObjectType.ORDERBY,useAggregation);
//			super(useAggregation);
//			sortQuery = new BasicDBObject();
//				
//		}
//
//		
//		public void add(Sort sort) {
//				
//			int sortType =  (sort.getType() == Sort.ASC) ? 1 : -1;	
//			sortQuery.put(sort.getField(), sortType);
//			
//		}
//		
//		public DBObject build(){
//			
//			sortQuery = QueryBuilder.start().get();
//			
//			DBObject container;
//			if(useAggregation){
//				container = new BasicDBObject(); 
//				container.put("$sort", sortQuery);
//			}else container = sortQuery;
//			
//			//log.debug(container.toString());
//			
//			return container;
//				
//		}
//	}
//
//
