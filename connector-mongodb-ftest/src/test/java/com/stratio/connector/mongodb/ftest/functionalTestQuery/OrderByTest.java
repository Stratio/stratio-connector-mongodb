package com.stratio.connector.mongodb.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.meta.Sort;
import com.stratio.connector.meta.StringTerm;
import com.stratio.connector.mongodb.core.engine.MongoQueryEngine;
import com.stratio.connector.mongodb.ftest.ConnectionTest;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
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

public class OrderByTest extends ConnectionTest {


    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    
    public static final int SORT_AGE = 1;
    public static final int SORT_AGE_MONEY = 2;
    public static final int SORT_AGE_TEXT = 3;


    @Test
    public void sortDescTest() throws Exception {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         
         
         LogicalPlan logicalPlan = createLogicalPlan(SORT_AGE);
         
         //return COLUMN_TEXT order by age DESC
         MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector.getQueryEngine()).execute(logicalPlan);
         
         
         assertEquals(5, queryResult.size());
         
        List<Row> resultList =  queryResult.getRows();
        
        assertEquals("text5", resultList.get(0).getCell(COLUMN_TEXT).getValue());
        assertEquals("text4", resultList.get(1).getCell(COLUMN_TEXT).getValue());
        assertEquals("text3", resultList.get(2).getCell(COLUMN_TEXT).getValue());
        assertEquals("text1", resultList.get(3).getCell(COLUMN_TEXT).getValue());
        assertEquals("text2", resultList.get(4).getCell(COLUMN_TEXT).getValue());

    }

    
    private LogicalPlan createLogicalPlan(int sortAge) {
    	Collection<Filter> coll = new HashSet<Filter>();
   	 
   	 
   	 List<LogicalStep> stepList = new ArrayList<>();

     List<ColumnMetadata> columns = new ArrayList<>();

     columns.add(new ColumnMetadata(COLLECTION,COLUMN_TEXT));
     columns.add(new ColumnMetadata(COLLECTION,COLUMN_AGE));
     Project project = new Project(CATALOG, COLLECTION,columns);
     stepList.add(project);
     
     
        switch (sortAge){
        	case SORT_AGE: stepList.add(new Sort(COLUMN_AGE, Sort.DESC)); break;
        	
// 2 Sort? o uno con lista de parámetros y luego lista de tipo ASC o DESC
//        	case SORT_AGE_MONEY: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
//        	case SORT_AGE_TEXT: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
        }
        return new LogicalPlan(stepList);

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
     Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN,RelationType.BETWEEN, relation);
     return f;
}


private Filter createEqualsFilter(int filterType, Object object) throws Exception {
	RelationCompare relCom;
	if(object instanceof String) relCom = new RelationCompare(COLUMN_TEXT, "=", new StringTerm((String)object));
	else if(object instanceof Integer) relCom = new RelationCompare(COLUMN_AGE, "=", new IntegerTerm(String.valueOf(object)));
	else throw new Exception("unsupported type"+ object.getClass());

	Filter f = new Filter(Operations.SELECT_WHERE_MATCH,RelationType.COMPARE, relCom);
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