package com.stratio.connector.mongodb.ftest.functionalMetadata;

import org.junit.Test;

import com.mongodb.DBCollection;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.mongodb.ftest.ConnectionTest;



public class CreateTest extends ConnectionTest {

	@Test(expected = com.stratio.connector.meta.exception.UnsupportedOperationException .class)  
	public void createTest() throws com.stratio.connector.meta.exception.UnsupportedOperationException {


		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);	
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createCatalog(CATALOG);

	}
	
	@Test(expected = com.stratio.connector.meta.exception.UnsupportedOperationException .class)  
	public void createCollectionTest() throws com.stratio.connector.meta.exception.UnsupportedOperationException {


		DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
				COLLECTION);	
		((IMetadataProvider) stratioMongoConnector.getMedatadaProvider()).createTable(CATALOG, COLLECTION);

	}
	
	
}