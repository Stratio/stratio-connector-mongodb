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
//
//package com.stratio.connector.mongodb.ftest.functionalTestQuery;
//
//import static org.junit.Assert.assertEquals;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.junit.Test;
//
//import com.mongodb.BasicDBObjectBuilder;
//import com.mongodb.DBCollection;
//import com.mongodb.MongoException;
//import com.stratio.connector.meta.GroupBy;
//import com.stratio.connector.meta.Limit;
//import com.stratio.connector.meta.MongoResultSet;
//import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
//import com.stratio.connector.mongodb.ftest.ConnectionTest;
//import com.stratio.meta.common.data.Row;
//import com.stratio.meta.common.logicalplan.LogicalPlan;
//import com.stratio.meta.common.logicalplan.LogicalStep;
//import com.stratio.meta.common.logicalplan.Project;
//import com.stratio.meta.common.metadata.structures.ColumnMetadata;
//
//
//public class GroupByTest extends ConnectionTest {
//
//	public static final String COLUMN_TEXT = "text";
//	public static final String COLUMN_AGE = "age";
//	public static final String COLUMN_MONEY = "money";
//
//	@Test
//	public void limitTest() throws Exception {
//
//		insertRow(1, "text", 10, 20);// row,text,money,age
//		insertRow(2, "text", 9, 17);
//		insertRow(3, "text", 11, 26);
//		insertRow(4, "text", 10, 30);
//		insertRow(5, "text", 20, 42);
//		insertRow(6, "text", 20, 48);
//		
//		LogicalPlan logicalPlan = createLogicalPlan(COLUMN_MONEY);
//
//		// group by money
//		MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector
//				.getQueryEngine()).execute(logicalPlan);
//
//		
//		assertEquals(4, queryResult.size());
//
//	}
//	
//	private LogicalPlan createLogicalPlan(String columnText) {
//
//		List<LogicalStep> stepList = new ArrayList<>();
//
//		List<ColumnMetadata> columns = new ArrayList<>();
//
//		columns.add(new ColumnMetadata(COLLECTION, COLUMN_MONEY));
//		columns.add(new ColumnMetadata(COLLECTION, COLUMN_AGE));
//		Project project = new Project(CATALOG, COLLECTION, columns);
//		stepList.add(project);
//
//		stepList.add(new GroupBy(columnText));
//
//		return new LogicalPlan(stepList);
//
//	}
//
//	private void insertRow(int ikey, String texto, int money, int age)
//			throws MongoException {
//
//		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
//				COLLECTION);
//		collection.insert(BasicDBObjectBuilder.start()
//				.append(COLUMN_TEXT, texto + ikey).append(COLUMN_MONEY, money)
//				.append(COLUMN_AGE, age).get());
//
//	}
//
//}