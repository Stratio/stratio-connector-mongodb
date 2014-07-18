package com.stratio.connector.mongodb.core.engine;

import java.nio.channels.UnsupportedAddressTypeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.meta.ICallBack;
import com.stratio.connector.meta.IResultSet;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.meta.Sort;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.relationships.RelationBetween;
import com.stratio.meta.common.statements.structures.relationships.RelationCompare;
import com.stratio.meta.common.statements.structures.relationships.RelationIn;
import com.stratio.meta.common.statements.structures.relationships.RelationType;
import com.stratio.meta.common.statements.structures.terms.Term;
import com.stratio.meta.common.statements.structures.window.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;


/**
 * Created by jmgomez on 10/07/14.
 */
public class MongoQueryEngine implements IQueryEngine {

    private MongoClient mongoClient = null;

    public IResultSet execute(LogicalPlan logicalPlan) throws UnsupportedOperationException, ExecutionException {
    	
    	boolean isAggregate = false;
    	DBCursor cursor=null;
    	
    	//cambiar
    	List<LogicalStep> logicalSteps = logicalPlan.getStepList();
    	Project projection = null;
    	ArrayList<Sort> sortList = new ArrayList<Sort>();
    	Limit limitValue = null;
    	ArrayList<Filter> filterList = new ArrayList<Filter>();
    	
    	
    	for (LogicalStep lStep : logicalSteps){ //validar??
    		if (lStep instanceof Project){
    			if(projection == null) projection = (Project) lStep;
    			else throw new ExecutionException(" # Project > 1");
    		}else if (lStep instanceof Sort){
    			sortList.add((Sort) lStep);
    		}else if (lStep instanceof Limit){
    			if(limitValue == null) limitValue = (Limit) lStep;
    			else throw new ExecutionException(" # Limit > 1");
    		}else if (lStep instanceof Filter){
    			filterList.add((Filter) lStep);
    		}else{
    			throw new UnsupportedOperationException("lStep.getType() unupported");
    		}
    	}
    	
    	
    	//comprobar que el orden sea el correcto?? comprobar si aggregate?
    	if(!isAggregate) {
    		
    		//if !isCount
    		//if !isDistinct
    		//if !isGroup
    		
    		
    		cursor = executeQuery(projection.getCatalogName(),projection.getTableName(), createQuery(logicalSteps),createFields(projection.getColumnList()));  	
    		
			
			
    		if(! sortList.isEmpty()) {
    			DBObject orderBy = new BasicDBObject();//asc o desc, y varios sort posibles => 
    			int sortType;
    			for(Sort sortElem: sortList){ //varios sort
    				sortType = (sortElem.getType()== Sort.ASC) ? 1 : -1;
    				orderBy.put(sortElem.getField(), sortType);
    			}	
        		
        		cursor = cursor.sort(orderBy);
    		}
    		if(limitValue != null){
    			cursor = cursor.limit(limitValue.getLimit());
    		}
    	
    	}
    	
    	
    	MongoResultSet resultSet = new MongoResultSet();
    	resultSet.setColumnMetadata(projection.getColumnList());//necesario??

		DBObject rowDBObject;
		
    	try{
			while(cursor.hasNext()){
				rowDBObject = cursor.next();			
				resultSet.add(createRow(rowDBObject));
				System.out.println(rowDBObject);
			}
    	}catch(MongoException e){
    		throw new ExecutionException("MongoException: "+e.getMessage());
    	}finally{
    		cursor.close();
    	}
		
    	
		return resultSet;

    }

    public IResultSet execute(IResultSet previousResult, LogicalPlan logicalPlan) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    public IResultSet execute(LogicalPlan logicalPlan, ICallBack callback) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    
	
	
	 /**
     * Queries for objects in a collection
     *
     * @param catalog 		the database.
	 * @param tablename		the collection.
	 * @param query 		the query.
	 * @param fields 		the projection.
     * @return the cursor. An iterator over results
     */
	 private DBCursor executeQuery(String catalog, String tablename, DBObject query, DBObject fields){
    	DB db = mongoClient.getDB(catalog);
		DBCollection coll = db.getCollection(tablename);
		return coll.find(query,fields);
    }
	
	
	 /**
     * This method creates a row from a mongoResult
     *
     * @param mongoResult the mongoResult.
     * @return the row.
     */
    private Row createRow(DBObject rowDBObject){
    	Row row = new Row();
    	for (String field : rowDBObject.keySet()){
    		row.addCell(field, new Cell(rowDBObject.get(field)));
    	}
		return row;
    }
    
    
    private DBObject createFields(List<ColumnMetadata> columnMetadata){
    	//primaryKey si necesaria sobrecargar método
    	
    	
    	DBObject fields = new BasicDBObject();
		
		if(columnMetadata == null || columnMetadata.isEmpty() ) {
    		//throw new ValidationException? select *
    		System.out.println("ValidException");
    	}else{
			for(ColumnMetadata colMetadata: columnMetadata){
				fields.put(colMetadata.getColumnName(), 1);//no comprobar columName...?
			}
			if(!fields.containsField("_id")) fields.put("_id", 0);
		}
    	
		return fields;
    	
    }
    
	//TODO a list of filters? Necesarios nuevos LogicalStep?
    private DBObject createQuery(List<LogicalStep> logicalSteps) {

		FilterDBObject fil = null;
		DBObject query = null;
		
    	//cambiar el orden de(deberían implementar todos una interfaz de operando con el nombre.
	
		for(Object o: logicalSteps){

			if( o instanceof Filter ){
				if( fil == null ) fil = new FilterDBObject();
				fil.addFilter((Filter) o);//pasar a or, and,...
				query = fil.getFilterQuery();
			}
		}
		System.out.println(query.toString());
		return query;
	}

	

	
	  /**
* Set the connection.
* @param aerospikeClient the connection.
*/
    public void setConnection(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }
}
