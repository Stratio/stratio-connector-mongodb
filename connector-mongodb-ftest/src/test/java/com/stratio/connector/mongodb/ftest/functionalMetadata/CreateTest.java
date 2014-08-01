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