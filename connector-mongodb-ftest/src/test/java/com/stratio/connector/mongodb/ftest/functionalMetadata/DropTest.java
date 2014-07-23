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