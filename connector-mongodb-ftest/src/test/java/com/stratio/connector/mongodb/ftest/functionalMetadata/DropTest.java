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
package com.stratio.connector.mongodb.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.mongodb.DBCollection;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.connector.mongodb.ftest.ConnectionTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;

public class DropTest extends ConnectionTest {

	@Test
	public void dropCollectionTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

		((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(
				CATALOG, COLLECTION, row);

		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);
		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).dropTable(CATALOG, COLLECTION);

		assertEquals("Catalog deleted", false, mongoClient.getDB(CATALOG).getCollectionNames().contains(COLLECTION) ); 

	}
	
	@Test
	public void dropCatalogTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

		((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(
				CATALOG, COLLECTION, row);

		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);
		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).dropCatalog(CATALOG);

		assertEquals("Catalog deleted", false, mongoClient.getDatabaseNames().contains(CATALOG) ); 

	}
	
	
}