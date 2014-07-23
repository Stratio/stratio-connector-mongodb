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
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.relationships.RelationBetween;
import com.stratio.meta.common.statements.structures.relationships.RelationCompare;
import com.stratio.meta.common.statements.structures.relationships.RelationType;
import com.stratio.meta.common.statements.structures.terms.IntegerTerm;
import com.stratio.meta.common.statements.structures.terms.Term;


/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryFilterTest extends ConnectionTest{

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER =1;
    private static final int BETWEEN_FILTER =2;
    private static final int HIGH_FILTER =3;
    private static final int LOW_FILTER =4;
    private static final int HIGH_BETWEEN_FILTER =5;

    @Test
    public void selectFilterEqual() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {


        insertRow(1,10,1);
        insertRow(2,9,1);
        insertRow(3,11,1);
        insertRow(4,10,1);
        insertRow(5,20,1);


        LogicalPlan logicalPlan = createLogicalPlan(EQUAL_FILTER);
       MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
       
       Set<Object> proveSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",6,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("money1"));



    }




    @Test
    public void selectFilterBetween() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {



    	insertRow(1,1,10);
    	insertRow(2,1,9);
    	insertRow(3,1,11);
    	insertRow(4,1,10);
    	insertRow(5,1,20);
    	insertRow(6,1,11);
    	insertRow(7,1,8);
    	insertRow(8,1,12);

        LogicalPlan logicalPlan = createLogicalPlan(BETWEEN_FILTER);
        MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",14,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money11"));
        assertTrue("Return correct record",proveSet.contains("age1"));

    }


   
    @Test
    public void selectHighBetween() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {



    	insertRow(1,10, 15);
    	insertRow(2,9,10);
    	insertRow(3,11,9);
    	insertRow(4,10,7);
    	insertRow(5,7,9);
    	insertRow(6,11,100);
    	insertRow(7,8,1);
    	insertRow(8,12,10);



        LogicalPlan logicalPlan = createLogicalPlan(HIGH_BETWEEN_FILTER);
        MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
            	System.out.println(cell+""+row.getCell(cell).getValue());
            	System.out.flush();
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",8,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("age12"));

    }



    @Test
    public void selectFilterHigh() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {



    	insertRow(1,10,1);
    	insertRow(2,9,1);
    	insertRow(3,11,1);
    	insertRow(4,10,1);
    	insertRow(5,20,1);
    	insertRow(6,7,1);
    	insertRow(7,8,1);
    	insertRow(8,12,1);


        LogicalPlan logicalPlan = createLogicalPlan(HIGH_FILTER);
        MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",15,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("age12"));
        assertTrue("Return correct record",proveSet.contains("age20"));
        assertTrue("Return correct record",proveSet.contains("money1"));




    }


    @Test
    public void selectFilterLow() throws MongoException, UnsupportedException, UnsupportedOperationException, ExecutionException {



    	insertRow(1,10,1);
    	insertRow(2,9,1);
    	insertRow(3,11,1);
    	insertRow(4,10,1);
    	insertRow(5,20,1);
    	insertRow(6,7,1);
    	insertRow(7,8,1);
    	insertRow(8,12,1);


        LogicalPlan logicalPlan = createLogicalPlan(LOW_FILTER);
        MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        for (Row row :queryResult.getRows()){
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct", 15, proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age9"));
        assertTrue("Return correct record",proveSet.contains("age7"));
        assertTrue("Return correct record",proveSet.contains("age8"));
        assertTrue("Return correct record",proveSet.contains("money1"));

    }


    private LogicalPlan createLogicalPlan(int filterType) {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_1));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_2));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_AGE));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_MONEY));

        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
        if (EQUAL_FILTER==filterType || HIGH_FILTER == filterType || LOW_FILTER == filterType || HIGH_BETWEEN_FILTER == filterType) stepList.add(createEqualsFilter(filterType));
        if (BETWEEN_FILTER==filterType || HIGH_BETWEEN_FILTER==filterType) stepList.add(createBetweenFilter());
        return new LogicalPlan(stepList);
    }

    private LogicalStep createBetweenFilter() {
        Relation relation = new RelationBetween(COLUMN_MONEY);
        relation.setType(Relation.TYPE_BETWEEN);
        List<Term<?>> terms = new ArrayList<>();
        terms.add(new IntegerTerm("9"));
        terms.add(new IntegerTerm("11"));
        relation.setTerms(terms);
        Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN, RelationType.BETWEEN, relation);
        return f;
    }

    private Filter createEqualsFilter(int filterType) {
        Relation relation = new RelationCompare(COLUMN_AGE);
        relation.setType(Relation.TYPE_COMPARE);
        if (filterType==EQUAL_FILTER)
            relation.setOperator("=");
        if (filterType==HIGH_FILTER || filterType == HIGH_BETWEEN_FILTER)
            relation.setOperator(">=");
        if (filterType==LOW_FILTER)
            relation.setOperator("<=");
        List<Term<?>> terms = new ArrayList<>();
        IntegerTerm term = new IntegerTerm("10");
        terms.add(term);
        relation.setTerms(terms);
        return new Filter( Operations.SELECT_WHERE_MATCH , RelationType.COMPARE, relation);
    }

    
        private void insertRow(int ikey, int age, int money) throws MongoException {
 	
            
            DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
            collection.insert(BasicDBObjectBuilder.start().
            		append(COLUMN_1,"ValueBin1_r"+ikey).
            		append(COLUMN_2,"ValueBin2_r"+ikey).
            		append(COLUMN_AGE,age).
            		append(COLUMN_3,"ValueBin3_r"+ikey).
            		append(COLUMN_MONEY,money).
            		get() );
            
        }


}
