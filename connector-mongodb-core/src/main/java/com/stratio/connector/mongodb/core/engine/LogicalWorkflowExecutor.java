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


package com.stratio.connector.mongodb.core.engine;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.mongodb.core.engine.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;

public class LogicalWorkflowExecutor {
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	private Project projection = null;
	//private ArrayList<Sort> sortList = null;
	//private Limit limitValue = null;
	private ArrayList<Filter> filterList = null;
	//private GroupBy groupBy = null;

	private boolean mandatoryAggregation; // true por defecto => si solo un RS,
											// mejora de rendimiento y no
											// necesario => check

	
	
	private List<DBObject> query = null;

	public LogicalWorkflowExecutor(LogicalWorkflow logicalWorkflow)
			throws  MongoQueryException, UnsupportedException {

		readLogicalWorkflow(logicalWorkflow);
		setMandatoryAggregation();
		buildQuery();

	}

	private void setMandatoryAggregation() {
		// if isGroupBy, Sum, Average, etc... isAggregate = true
		// TODO update to new LogicalSteps
		mandatoryAggregation = true;

	}

	private void readLogicalWorkflow(LogicalWorkflow logicalWorkflow) throws MongoQueryException, UnsupportedException{

		
		//TODO getLastStep?? should be getSteps(); 
		
		List<LogicalStep> logicalSteps = logicalWorkflow.getInitialSteps();
		
		//sortList = new ArrayList<Sort>();
		filterList = new ArrayList<Filter>();

		for (LogicalStep lStep : logicalSteps) { //TODO validar??
			if (lStep instanceof Project) {
				if (projection == null)
					projection = (Project) lStep;
				else
					throw new UnsupportedException(" # Project > 1");
			}else if (lStep instanceof Filter) {
				filterList.add((Filter) lStep);
			}/* else if (lStep instanceof Sort) {
				sortList.add((Sort) lStep);
			} else if (lStep instanceof Limit) {
				if (limitValue == null)
					limitValue = (Limit) lStep;
				else
					throw new MongoQueryException(" # Limit > 1", null);
			}  else if (lStep instanceof GroupBy) {
				if (groupBy == null)
					groupBy = (GroupBy) lStep;
				else
					throw new MongoQueryException(" # GroupBy > 1", null);
			}*/ else {
				throw new UnsupportedException("step unsupported" + lStep.getClass());
			}
		}
		
		if (projection == null) throw new MongoQueryException("projection has not been found",null);

	}

	public boolean isMandatoryAggregation() {
		return mandatoryAggregation;
	}

	private void buildQuery() {
		query = new ArrayList<DBObject>();

		if (mandatoryAggregation) {
			if(!filterList.isEmpty()) query.add(buildFilter());
			// TODO Orden actual: project y después group by => si llegarán =>comprobar
			query.add(buildProject());
			//if (groupBy != null) query.add(buildGroupBy());
			//if (!sortList.isEmpty()) query.add(buildSort());
			//if (limitValue != null) query.add(buildLimit());

		}

		// check order?? check aggregation?
		// SOLO AÑADIR LA QUERY
		// else=> sin usar el framework de agregación.
		else {
				query.add(buildFilter());
		}

	
		

	}

	/*
	private DBObject buildLimit() {
		LimitDBObjectBuilder limitDBObject = new LimitDBObjectBuilder(limitValue);	
		return limitDBObject.build();
	}

	private DBObject buildSort() {
		SortDBObjectBuilder sortDBObject = new SortDBObjectBuilder(mandatoryAggregation);							
		for (Sort sortElem : sortList) { 
			sortDBObject.add(sortElem);
		}
		return sortDBObject.build();
	}

	private DBObject buildGroupBy() {
		// GROUP(con avg, sum, max, etc...) , SORT, LIMIT ....(comprobar
		// rendimiento...)

		GroupByDBObjectBuilder groupDBObject = new GroupByDBObjectBuilder(
				groupBy);

		// SelectorIdentifier selIdentifier = groupBy
		// .getSelectorIdentifier();
		// selIdentifier.getType()

		// if (selIdentifier.getType() == SelectorMeta.TYPE_GROUPBY) {
		// final String field = selIdentifier.getField();
		//
		// dBObject (group, new DBObject(id, new
		// DBObject(campo1:$campo1,campo2:$campo2, agregación(como abajo) ) =>EJ
		// donde se agrupa por muchos campos
		// dbObject (group, new DBObject(id, (contador, new DBObject( $sum: 1)),
		// (sumaValores: new DBObject( $sum:$campoasumar)))
		// count =>group con id_:null
		//
		// DBObject groupBy = new BasicDBObject();//asc o desc, y
		// varios sort posibles =>
		// int sortType;
		// for(Sort sortElem: sortList){ //varios sort
		// sortType = (sortElem.getType()== Sort.ASC) ? 1 : -1;
		// orderBy.put(sortElem.getField(), sortType);
		// }
		// System.out.println(new
		// BasicDBObject("$sort",orderBy).toString());
		// pipeline.add(new BasicDBObject("$sort",orderBy));
		// }

		return groupDBObject.build();
	}
	*/
	
	private DBObject buildProject() {
		ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(
				mandatoryAggregation, projection);
		return projectDBObject.build();
	}

	private DBObject buildFilter() {

			FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(mandatoryAggregation);
			for (Filter f : filterList) {
				filterDBObjectBuilder.add(f);
			}
			if(logger.isDebugEnabled()){
				logger.debug("ConsultaAgg" + filterDBObjectBuilder.build());
			}
			return filterDBObjectBuilder.build();
	}

	/**
	 * Queries for objects in a collection
	 */

	public MongoResultSet executeQuery(MongoClient mongoClient) {

		DB db = mongoClient.getDB(projection.getCatalogName());
		DBCollection coll = db.getCollection(projection.getTableName().getName());
		MongoResultSet resultSet = new MongoResultSet();
		//resultSet.setColumnMetadata(projection.getColumnList());// necesario??

		if (isMandatoryAggregation()) {

			// AggregationOptions aggOptions = AggregationOptions.builder()
			// .allowDiskUse(true)
			// .batchSize(size)
			// .maxTime(maxTime, timeUnit)
			// .outputMode(OutputMode.CURSOR) or INLINE
			// .build();
			// pipeline,aggOptions => dbcursor

			int stage =1;
			for (DBObject aggregationStage: query){
				logger.debug("Aggregate framework stage ("+ (stage++) +") : " +aggregationStage.toString());
			}
			
			
			AggregationOutput aggOutput = coll.aggregate(query);

			for (DBObject result : aggOutput.results()) {
				logger.debug("AggResult: " + result);
				resultSet.add(createRow(result));
			}

		} else {

//			ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(
//					false, projection);
//			DBObject fields = projectDBObject.build();
			DBObject fields = buildProject();
			
			
			// if !isCount
			// if !isDistinct
			// if !groupBy

			DBCursor cursor = coll.find(query.get(0), fields);
			DBObject rowDBObject;

			/*
			// sort, skip and limit
			if (!sortList.isEmpty()) {
//				DBObject orderBy = new BasicDBObject();// asc o desc, y varios
//														// sort posibles =>
//				int sortType;
//				for (Sort sortElem : sortList) { // varios sort
//					sortType = (sortElem.getType() == Sort.ASC) ? 1 : -1;
//					orderBy.put(sortElem.getField(), sortType);
//				}
				DBObject orderBy = buildSort();

				cursor = cursor.sort(orderBy);
			}
			if (limitValue != null) {
				cursor = cursor.limit(limitValue.getLimit());
			}
			*/
			// iterate over the cursor
			try {
				while (cursor.hasNext()) { //Si no hay resultados => excepción..
					rowDBObject = cursor.next();
					resultSet.add(createRow(rowDBObject));
				}
			} catch (MongoException e) {
				// throw new ExecutionException("MongoException: "
				// + e.getMessage());
			} finally {
				cursor.close();
			}
		}
		return resultSet;

	}
	
	
	
	/**
	 * This method creates a row from a mongoResult
	 *
	 * @param mongoResult
	 *            the mongoResult.
	 * @return the row.
	 */
	private Row createRow(DBObject rowDBObject) {
		Row row = new Row();
		for (String field : rowDBObject.keySet()) {
			row.addCell(field, new Cell(rowDBObject.get(field)));
		}
		return row;
	}

}
