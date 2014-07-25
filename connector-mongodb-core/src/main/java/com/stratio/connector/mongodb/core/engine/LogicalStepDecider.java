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
import com.stratio.connector.meta.GroupBy;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.MongoResultSet;
import com.stratio.connector.meta.Sort;
import com.stratio.connector.mongodb.core.engine.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.GroupByDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.LimitDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.SortDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.connector.mongodb.core.exceptions.MongoUnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;

public class LogicalStepDecider {
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());

	private Project projection = null;
	private ArrayList<Sort> sortList = null;
	private Limit limitValue = null;
	private ArrayList<Filter> filterList = null;
	private GroupBy groupBy = null;

	private boolean mandatoryAggregation; // true por defecto => si solo un RS,
											// mejora de rendimiento y no
											// necesario => check

	
	
	private List<DBObject> query = null;

	public LogicalStepDecider(LogicalPlan logicalPlan)
			throws MongoUnsupportedOperationException, MongoQueryException {

		readLogicalPlan(logicalPlan);
		setMandatoryAggregation();
		buildQuery();

	}

	private void setMandatoryAggregation() {
		// if isGroupBy, Sum, Average, etc... isAggregate = true
		mandatoryAggregation = true;

	}

	private void readLogicalPlan(LogicalPlan logicalPlan)
			throws MongoQueryException, MongoUnsupportedOperationException {

		List<LogicalStep> logicalSteps = logicalPlan.getStepList();
		sortList = new ArrayList<Sort>();
		filterList = new ArrayList<Filter>();

		for (LogicalStep lStep : logicalSteps) { // validar??
			if (lStep instanceof Project) {
				if (projection == null)
					projection = (Project) lStep;
				else
					throw new MongoQueryException(" # Project > 1", null);
			} else if (lStep instanceof Sort) {
				sortList.add((Sort) lStep);
			} else if (lStep instanceof Limit) {
				if (limitValue == null)
					limitValue = (Limit) lStep;
				else
					throw new MongoQueryException(" # Limit > 1", null);
			} else if (lStep instanceof Filter) {
				filterList.add((Filter) lStep);
			} else if (lStep instanceof GroupBy) {
				if (groupBy == null)
					groupBy = (GroupBy) lStep;
				else
					throw new MongoQueryException(" # GroupBy > 1", null);
			} else {
				throw new UnsupportedOperationException("type unsupported");
			}
		}
		
		if (projection == null) throw new MongoQueryException("no projection founded",null);

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
			if (groupBy != null) query.add(buildGroupBy());
			if (!sortList.isEmpty()) query.add(buildSort());
			if (limitValue != null) query.add(buildLimit());

		}

		// comprobar que el orden sea el correcto?? comprobar si aggregate?
		// //SOLO AÑADIR LA QUERY
		// else=> sin usar el framework de agregación.
		else {
				query.add(buildFilter());
		}

		

	}

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
			logger.debug("ConsultaAgg" + filterDBObjectBuilder.build());
			return filterDBObjectBuilder.build();
	}

	/**
	 * Queries for objects in a collection
	 */

	public MongoResultSet executeQuery(MongoClient mongoClient) {

		DB db = mongoClient.getDB(projection.getCatalogName());
		DBCollection coll = db.getCollection(projection.getTableName());
		MongoResultSet resultSet = new MongoResultSet();
		resultSet.setColumnMetadata(projection.getColumnList());// necesario??

		if (isMandatoryAggregation()) {

			// AggregationOptions aggOptions = AggregationOptions.builder()
			// .allowDiskUse(true)
			// .batchSize(size)
			// .maxTime(maxTime, timeUnit)
			// .outputMode(OutputMode.CURSOR) or INLINE
			// .build();
			// pipeline,aggOptions => dbcursor

			AggregationOutput aggOutput = coll.aggregate(query);

			resultSet = new MongoResultSet();

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
