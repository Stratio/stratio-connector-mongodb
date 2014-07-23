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

public class IndexTest extends ConnectionTest{
	
	@Test
	public void testCreateIndex() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell("value2"));
		cells.put("name3", new Cell("value3"));
		row.setCells(cells);

		((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);

		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);
		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "name1");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "name2");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "value1");

		
		assertEquals("Index created", 4, collection.getIndexInfo().size()); //id + 3
	
//		assertEquals("Index created", 3, collection.getIndexInfo().g);
//		assertEquals("Index created", true, collection.getIndexInfo().contains("name2"));
//		assertEquals("Index created", true, collection.getIndexInfo().contains("value1")); //evitar que se cree si no existe el campo?

	}
	
	@Test
	public void testDropIndex() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException {




		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);
		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index1");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index2");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index3");

		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).dropIndex(CATALOG, COLLECTION, "index2");
		
		assertEquals("Index created", 3, collection.getIndexInfo().size());


	}
	
	
	@Test
	public void testDropIndexes() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException {




		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);
		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index1");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index2");
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createIndex(CATALOG, COLLECTION, "index3");

		
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).dropIndexes(CATALOG, COLLECTION);
		
		assertEquals("Index created", 1, collection.getIndexInfo().size());


	}
	
}
