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
package com.stratio.connector.mongodb.ftest.functionalInsert;


import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.functionalInsert.GenericSimpleInsertTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 16/07/14.
 */
public class SimpleInsertTest extends GenericSimpleInsertTest {

	@Override
	protected IConnectorHelper getConnectorHelper() {
		MongoConnectorHelper mongoConnectorHelper = null;
	try {
		mongoConnectorHelper = new MongoConnectorHelper(getClusterName());
	} catch (ConnectionException e) {
		e.printStackTrace();
	} catch (InitializationException e) {
		e.printStackTrace();
	} catch (CreateNativeConnectionException e) {
		e.printStackTrace();
	}
		return mongoConnectorHelper;
	}


    
//    @Test
//    public void booleanSimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        cells.put("name1", new Cell(new Boolean(true)));
//        cells.put("name2", new Cell(false));
//        row.setCells(cells);
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//            assertEquals("The value is correct", true, resultSet.get("name1"));
//            assertEquals("The value is correct", new Boolean(false), resultSet.get("name2"));
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 1, recordNumber);
//
//    }
//    
//    @Test
//    public void numberSimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        cells.put("name1", new Cell(new Long(Long.MAX_VALUE)));
//        cells.put("name2", new Cell(new Float(23.5)));
//        cells.put("name3", new Cell(23.5f));
//        cells.put("name4", new Cell(new Double(23.5)));
//        row.setCells(cells);
//        
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//            assertEquals("The value is correct", Long.MAX_VALUE, resultSet.get("name1"));
////            assertEquals("The value is correct", 23.5f, resultSet.get("name2"));
////            assertEquals("The value is correct", 23.5f, resultSet.get("name3"));
//            assertEquals("The value is correct", 23.5, resultSet.get("name4"));
//            
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 1, recordNumber);
//
//    }
//    
//    @Test
//    public void arraySimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        
//        String[] arrayString = new String("a:b:c:d").split(":");
//        cells.put("name1", new Cell( arrayString));
//        
//        List<Integer> al = Arrays.asList(1,2,3,4);
//        cells.put("name2", new Cell(al));
//        
////        HashSet<String> setString = new HashSet<String>();
////        setString.add("a");
////        setString.add("b");
////        cells.put("name3", new Cell(setString));
//
//        
//        Map<String,String> map = new HashMap<String,String>();
//        map.put("a","a1");
//        map.put("b","b1");
//        cells.put("name4", new Cell(map));
//        
//        Map<String,Map<String,String>> nestedMap = new HashMap<String,Map<String,String>>();
//        nestedMap.put("map1",map);
//        nestedMap.put("map2",map);
//        cells.put("name5", new Cell(nestedMap));
//        
//        row.setCells(cells);
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//            assertEquals("The value is correct", "b", ((BasicDBList) resultSet.get("name1")).get(1) );
//            assertEquals("The value is correct", 4, ((BasicDBList) resultSet.get("name1")).size());
//            
//            assertEquals("The value is correct", 2,  ((BasicDBList) resultSet.get("name2")).get(1) );
//            assertEquals("The value is correct", 4, ((BasicDBList) resultSet.get("name2")).size() );
//            
//           
//            assertEquals("The value is correct", "a1", ((DBObject) resultSet.get("name4")).get("a") );     
//            assertEquals("The value is correct", "b1", ((DBObject) resultSet.get("name4")).get("b"));   
////          assertEquals("The value is correct", "a1",  (resultSet.get("name4.a")));
//            
//            
//            assertEquals("The value is correct", "a1", ((DBObject) ((DBObject) resultSet.get("name5")).get("map1")).get("a") );     
//            
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 1, recordNumber);
//
//    }
//    
//    @Test
//    public void overwriteSimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        cells.put("name1", new Cell("value1"));
//        cells.put("name2", new Cell("val"));
//        cells.put("name3", new Cell("val"));
//        row.setCells(cells);
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//        
//        
//        Row row2 = new Row();
//        Map<String, Cell> cells2 = new HashMap<>();
//        cells.put("name1", new Cell("value2"));
//        cells.put("name2", new Cell(""));
//        cells.put("name3", new Cell(null));
//        row2.setCells(cells2);
//        
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row2);
//        
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//            
//        	if(recordNumber ==0){
//        		assertEquals("The value is correct", "value1", resultSet.get("name1"));
//	            assertEquals("The value is correct", "val", resultSet.get("name2"));
//	            assertEquals("The value is correct", "val", resultSet.get("name3"));
//        	}else if(recordNumber==2){
//        		assertEquals("The value is correct", "value2", resultSet.get("name1"));
//	            assertEquals("The value is correct", "", resultSet.get("name2"));
//	            assertEquals("The value is correct", null, resultSet.get("name3"));
//        	}
//            
//            
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 2, recordNumber);
//
//    }
//    
//    
//    @Test
//    public void uuidSimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        cells.put("name1", new Cell(new Boolean(true)));
//        cells.put("name2", new Cell(false));
//        row.setCells(cells);
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//            assertEquals("The value is correct", true, resultSet.get("name1"));
//            assertEquals("The value is correct", new Boolean(false), resultSet.get("name2"));
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 1, recordNumber);
//
//    }
//    
//    @Test
//    public void dateSimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//    	//Date y BSONTimestamp
//        Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//       
//        cells.put("name1", new Cell(GregorianCalendar.getInstance().getTime()));
//        row.setCells(cells);
//
//        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
//
//        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
//       
//        DBCursor cursor = collection.find();
//
//        DBObject resultSet;
//        int recordNumber = 0;
//        
//        while (cursor.hasNext()) {
//        	resultSet = cursor.next();
//           // assertEquals("The value is correct", GregorianCalendar.getInstance().getTime().getYear(), ((Date)resultSet.get("name1")).getYear() );
//            recordNumber++;
//        }
//        assertEquals("The records number is correct", 1, recordNumber);
//
//    }
//    
//    @Test
//    public void binarySimpleInsert() throws ExecutionException, ValidationException, MongoException {
//
//    	//byte[]
//    	//binary
//    	//grid??
//
////        Row row = new Row();
////        Map<String, Cell> cells = new HashMap<>();
////        cells.put("name1", new Cell(new Boolean(true)));
////        cells.put("name2", new Cell(false));
////        row.setCells(cells);
////
////        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
////
////        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
////       
////        DBCursor cursor = collection.find();
////
////        DBObject resultSet;
////        int recordNumber = 0;
////        
////        while (cursor.hasNext()) {
////        	resultSet = cursor.next();
////            assertEquals("The value is correct", true, resultSet.get("name1"));
////            assertEquals("The value is correct", new Boolean(false), resultSet.get("name2"));
////            recordNumber++;
////        }
////        assertEquals("The records number is correct", 1, recordNumber);
//
//    }

}
