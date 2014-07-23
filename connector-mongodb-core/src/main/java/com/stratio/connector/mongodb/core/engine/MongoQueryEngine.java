package com.stratio.connector.mongodb.core.engine;

import java.nio.channels.UnsupportedAddressTypeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.AggregationOptions.OutputMode;
import com.stratio.connector.meta.GroupBy;
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
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;
import com.stratio.meta.common.statements.structures.selectors.SelectorMeta;
import com.stratio.meta.common.statements.structures.terms.Term;
import com.stratio.meta.common.statements.structures.window.Window;
import com.stratio.meta.common.statements.structures.window.WindowType;


/**
 * Created by jmgomez on 10/07/14.
 */
public class MongoQueryEngine implements IQueryEngine {

    private MongoClient mongoClient = null;

    public IResultSet execute(LogicalPlan logicalPlan) throws UnsupportedOperationException, ExecutionException {
    	
    	MongoResultSet resultSet =null;
    	LogicalStepDecider decider = new LogicalStepDecider(logicalPlan);
    	resultSet = decider.executeQuery(mongoClient);  		
    	return resultSet; 	

    }

    public IResultSet execute(IResultSet previousResult, LogicalPlan logicalPlan) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    public IResultSet execute(LogicalPlan logicalPlan, ICallBack callback) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    

	
	  /**
* Set the connection.
* @param aerospikeClient the connection.
*/
    public void setConnection(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }
}
