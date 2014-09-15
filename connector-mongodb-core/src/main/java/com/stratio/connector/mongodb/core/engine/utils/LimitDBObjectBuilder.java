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
//import com.mongodb.BasicDBObject;
//import com.mongodb.DBObject;
//import com.stratio.connector.meta.Limit;
//
//public class LimitDBObjectBuilder extends DBObjectBuilder {
//
//	int limit;
//	
//	public LimitDBObjectBuilder(Limit limit) {
//		super(DBObjectType.LIMIT, false); //only with aggregationFramework
//		this.limit = limit.getLimit();
//	}
//
//	
//
//	@Override
//	public DBObject build() {
//		return new BasicDBObject("$limit",limit);
//	}
//
//}
