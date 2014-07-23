package com.stratio.connector.mongodb.ftest.functionalDelete;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.stratio.connector.meta.StringTerm;
import com.stratio.connector.mongodb.core.engine.MongoStorageEngine;
import com.stratio.connector.mongodb.ftest.ConnectionTest;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.relationships.RelationBetween;
import com.stratio.meta.common.statements.structures.relationships.RelationCompare;
import com.stratio.meta.common.statements.structures.relationships.RelationType;
import com.stratio.meta.common.statements.structures.terms.IntegerTerm;
import com.stratio.meta.common.statements.structures.terms.Term;



public class DeleteTest extends ConnectionTest {


    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER =1;
    
//    private static final int BETWEEN_FILTER =2;
//    private static final int HIGH_FILTER =3;
//    private static final int LOW_FILTER =4;
//    private static final int HIGH_BETWEEN_FILTER =5;
    private static final int NOTEQUAL_BETWEEN=6;

    @Test
    public void deleteFilterEqualString() throws Exception {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         

        Collection<Filter> filterSet = createFilterCollection(EQUAL_FILTER, "text2");
        
   	 	((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).delete(CATALOG, COLLECTION, filterSet);
        
        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        
        DBCursor cursor = collection.find();
        DBObject record = null;
        int numRecord = 0;
        
        while(cursor.hasNext()){
        	record = cursor.next();
        	assertEquals(false, record.get(COLUMN_TEXT).equals("text2"));
        	numRecord++;
        }
        assertEquals(4, numRecord);


    }

    
    @Test
    public void deleteFilterEqualInt() throws Exception {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,20);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         

        Collection<Filter> filterSet = createFilterCollection(EQUAL_FILTER, 20);
        
   	 	((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).delete(CATALOG, COLLECTION, filterSet);
        
        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        
        DBCursor cursor = collection.find();
        DBObject record = null;
        int numRecord = 0;
        
        while(cursor.hasNext()){
        	record = cursor.next();
        	assertEquals(false, record.get(COLUMN_AGE).equals(20));
        	numRecord++;
        }
        assertEquals(3, numRecord);


    }
    
    @Test
    public void deleteFilterNotEqualInt() throws Exception {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,20);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         

        Collection<Filter> filterSet = createFilterCollection(NOTEQUAL_BETWEEN, 20);//age notequal 20 and money between 9,11
        
   	 	((MongoStorageEngine) stratioMongoConnector.getStorageEngine()).delete(CATALOG, COLLECTION, filterSet);
        
        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        
        DBCursor cursor = collection.find();
        DBObject record = null;
        int numRecord = 0;
        
        while(cursor.hasNext()){
        	record = cursor.next();
        	assertEquals(false, record.get(COLUMN_AGE).equals(17) || record.get(COLUMN_AGE).equals(30));
        	numRecord++;
        }
        assertEquals(3, numRecord);


    }
    
    
 private Collection<Filter> createFilterCollection(int filterType, Object object) throws Exception {
	 
	 Collection<Filter> coll = new HashSet<Filter>();
	 
	 
	 List<LogicalStep> stepList = new ArrayList<>();
     List<ColumnMetadata> columns = new ArrayList<>();

     switch (filterType){
     	case EQUAL_FILTER: coll.add(createEqualsFilter(filterType, object)); break;
     	case NOTEQUAL_BETWEEN: coll.add(createNotEqualsFilter(filterType, object)); coll.add(createBetweenFilter(9,11)); break;
     }
//     if (EQUAL_FILTER==filterType 
//    		 || HIGH_FILTER == filterType
//    		 || LOW_FILTER == filterType
//    		 || HIGH_BETWEEN_FILTER == filterType) 
//    	 coll.add(createEqualsFilter(filterType, object));
//     if (BETWEEN_FILTER==filterType || HIGH_BETWEEN_FILTER==filterType) coll.add(createBetweenFilter());
     return coll;
     

	}


private Filter createNotEqualsFilter(int filterType, Object object) throws Exception {
	RelationCompare relCom;
	if(object instanceof String) relCom = new RelationCompare(COLUMN_TEXT, "!=", new StringTerm((String)object));
	else if(object instanceof Integer) relCom = new RelationCompare(COLUMN_AGE, "<>", new IntegerTerm(String.valueOf(object)));
	else throw new Exception("unsupported type"+ object.getClass());

	Filter f = new Filter(Operations.SELECT_WHERE_MATCH, RelationType.COMPARE, relCom);
	return f;
}


private Filter createBetweenFilter(int min, int max) {
	
	 Relation relation = new RelationBetween(COLUMN_MONEY);
     relation.setType(Relation.TYPE_BETWEEN);
     List<Term<?>> terms = new ArrayList<>();
     terms.add(new IntegerTerm(String.valueOf(min)));
     terms.add(new IntegerTerm(String.valueOf(max)));
     relation.setTerms(terms);
     Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN, RelationType.BETWEEN, relation);
     return f;
}


private Filter createEqualsFilter(int filterType, Object object) throws Exception {
	RelationCompare relCom;
	if(object instanceof String) relCom = new RelationCompare(COLUMN_TEXT, "=", new StringTerm((String)object));
	else if(object instanceof Integer) relCom = new RelationCompare(COLUMN_AGE, "=", new IntegerTerm(String.valueOf(object)));
	else throw new Exception("unsupported type"+ object.getClass());

	Filter f = new Filter(Operations.SELECT_WHERE_MATCH, RelationType.COMPARE, relCom);
	return f;
}


private void insertRow(int ikey, String texto, int money, int age) throws MongoException {
     	
        
        DBCollection collection = mongoClient.getDB(CATALOG).getCollection(COLLECTION);
        collection.insert(BasicDBObjectBuilder.start().
        		append(COLUMN_TEXT,texto+ikey).
        		append(COLUMN_MONEY,money).
        		append(COLUMN_AGE,age).
        		get() );
        
    }
 

 
 
 
}