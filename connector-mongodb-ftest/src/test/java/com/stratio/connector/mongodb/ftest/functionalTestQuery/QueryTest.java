package com.stratio.connector.mongodb.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.ftest.ConnectionTest;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;



/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryTest extends ConnectionTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    @Test
    public void selectAllFromTable() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {


        insertRow(1);
        insertRow(2);
        insertRow(3);
        insertRow(4);
        
        

        LogicalPlan logicalPlan = createLogicalPlan();
        MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> probeSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
                probeSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",12,probeSet.size());
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",probeSet.contains("bin3ValueBin3_r1"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",probeSet.contains("bin3ValueBin3_r2"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",probeSet.contains("bin3ValueBin3_r3"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",probeSet.contains("bin3ValueBin3_r4"));

    }






    private LogicalPlan createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_1));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_2));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_3));
        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
        return new LogicalPlan(stepList);
    }

    private void insertRow(int ikey) throws MongoException {
     	
        
        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        collection.insert(BasicDBObjectBuilder.start().
        		append(COLUMN_1,"ValueBin1_r"+ikey).
        		append(COLUMN_2,"ValueBin2_r"+ikey).
        		append(COLUMN_3,"ValueBin3_r"+ikey).
        		get() );
        
    }


}
