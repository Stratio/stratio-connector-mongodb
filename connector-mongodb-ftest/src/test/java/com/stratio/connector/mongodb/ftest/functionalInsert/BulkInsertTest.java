package com.stratio.connector.mongodb.ftest.functionalInsert;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.connector.mongodb.ftest.ConnectionTest;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.ValidationException;

/**
 * Created by jmgomez on 16/07/14.
 */
public class BulkInsertTest extends ConnectionTest {

    final String COLLECTION = getClass().getSimpleName();

    @Test
    public void testBulkInsert() throws ExecutionException, ValidationException, MongoException {


        Set<Row> rows = new HashSet<Row>();
        
        for (int i = 0; i < 10; i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put("key", new Cell(i));
            cells.put("name1", new Cell("value1_R" + i));
            cells.put("name2", new Cell("value2_R" + i));
            cells.put("name3", new Cell("value3_R" + i));
            row.setCells(cells);
            rows.add(row);
        }

        ((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).insert(CATALOG, COLLECTION, rows);
       

        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        
        DBCursor cursor = collection.find();

        DBObject resultSet;
        int recordNumber = 0;
        
        while (cursor.hasNext()) {
        	resultSet = cursor.next();
            assertEquals("The value is correct", "value1_R" + resultSet.get("key"), resultSet.get("name1"));
            assertEquals("The value is correct", "value2_R" + resultSet.get("key"), resultSet.get("name2"));
            assertEquals("The value is correct", "value3_R" + resultSet.get("key"), resultSet.get("name3"));
            recordNumber++;
        }
        


        assertEquals("The records number is correct", 10, recordNumber);

    }
}
