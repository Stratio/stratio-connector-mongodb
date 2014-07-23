package com.stratio.connector.mongodb.core.engine;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
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
import com.stratio.connector.mongodb.core.engine.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.connector.mongodb.core.exceptions.MongoUnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;
import com.stratio.meta.common.statements.structures.selectors.SelectorMeta;

public class LogicalStepDecider {

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
				throw new UnsupportedOperationException(
				/* lStep.getType()+ */"type unsupported");
			}
		}

	}

	public boolean isMandatoryAggregation() {
		return mandatoryAggregation;
	}

	private void buildQuery() {
		query = new ArrayList<DBObject>();

		if (mandatoryAggregation) {

			if (filterList != null || !filterList.isEmpty()) {

				FilterDBObjectBuilder filterDBObject = new FilterDBObjectBuilder(
						true);

				for (Filter f : filterList) {
					filterDBObject.addFilter(f);
				}

				System.out.println("ConsultaAgg" + filterDBObject.build());
				query.add(filterDBObject.build());
			}

			// TODO Orden actual: project y después group by => si llegarán =>
			// comprobar
			ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(
					true, projection);
			query.add(projectDBObject.build());

			// GROUP(con avg, sum, max, etc...) , SORT, LIMIT ....(comprobar
			// rendimiento...)

			//
			if (groupBy != null) {// CAMBIAR A GROUPDBObjectBuilder
				SelectorIdentifier selIdentifier = groupBy
						.getSelectorIdentifier();
				if (selIdentifier.getType() == SelectorMeta.TYPE_GROUPBY) {
					final String field = selIdentifier.getField();

					// dBObject (group, new DBObject(id, new
					// DBObject(campo1:$campo1,campo2:$campo2, agregación(como
					// abajo) ) =>EJ donde se agrupa por muchos campos
					// dbObject (group, new DBObject(id, (contador, new
					// DBObject( $sum: 1)), (sumaValores: new DBObject( $sum:
					// $campoasumar)))
					// count =>group con id_:null

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
				}
			}

			//
			if (!sortList.isEmpty()) { // CAMBIAR A SORTDBObjectBuilder
				DBObject orderBy = new BasicDBObject();// asc o desc, y varios
														// sort posibles =>
				int sortType;
				for (Sort sortElem : sortList) { // varios sort
					sortType = (sortElem.getType() == Sort.ASC) ? 1 : -1;
					orderBy.put(sortElem.getField(), sortType);
				}

				System.out.println(new BasicDBObject("$sort", orderBy)
						.toString());
				query.add(new BasicDBObject("$sort", orderBy));
			}
			if (limitValue != null) {// CAMBIAR A LIMITDBObjectBuilder
				query.add(new BasicDBObject("$limit", limitValue.getLimit()));
			}

		}

		// comprobar que el orden sea el correcto?? comprobar si aggregate?
		// //SOLO AÑADIR LA QUERY
		// else=> sin usar el framework de agregación
		else {

			if (filterList != null | !filterList.isEmpty()) {

				FilterDBObjectBuilder filterDBObject = new FilterDBObjectBuilder(
						false);

				for (Filter f : filterList) {
					filterDBObject.addFilter(f);
				}

				System.out.println("ConsultaAgg" + filterDBObject.build());
				query.add(filterDBObject.build());
			}

		}

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
				System.out.println("AggResult: " + result);
				resultSet.add(createRow(result));

			}

		} else {

			ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(
					false, projection);
			DBObject fields = projectDBObject.build();

			// if !isCount
			// if !isDistinct
			// if !groupBy

			DBCursor cursor = coll.find(query.get(0), fields);
			DBObject rowDBObject;

			// sort, skip and limit
			if (!sortList.isEmpty()) {
				DBObject orderBy = new BasicDBObject();// asc o desc, y varios
														// sort posibles =>
				int sortType;
				for (Sort sortElem : sortList) { // varios sort
					sortType = (sortElem.getType() == Sort.ASC) ? 1 : -1;
					orderBy.put(sortElem.getField(), sortType);
				}

				cursor = cursor.sort(orderBy);
			}
			if (limitValue != null) {
				cursor = cursor.limit(limitValue.getLimit());
			}

			// iterate over the cursor
			try {
				while (cursor.hasNext()) {
					rowDBObject = cursor.next();
					resultSet.add(createRow(rowDBObject));
					System.out.println(rowDBObject);
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
