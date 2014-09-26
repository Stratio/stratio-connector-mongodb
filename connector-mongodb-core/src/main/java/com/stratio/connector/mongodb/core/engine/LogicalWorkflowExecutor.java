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
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.mongodb.core.engine.utils.FilterDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.LimitDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.utils.ProjectDBObjectBuilder;
import com.stratio.connector.mongodb.core.exceptions.MongoQueryException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Limit;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.QualifiedNames;

public class LogicalWorkflowExecutor {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Project projection = null;
    private ArrayList<Filter> filterList = null;
    private Limit limit = null;
    private Select select = null;
    private boolean mandatoryAggregation; // true por defecto => si solo un RS,
                                          // mejora de rendimiento y no
                                          // necesario => check

    private List<DBObject> query = null;

    public LogicalWorkflowExecutor(LogicalStep initiallogicalStep) throws MongoQueryException, UnsupportedException {

        readLogicalWorkflow(initiallogicalStep);
        setMandatoryAggregation();
        buildQuery();

    }

    private void setMandatoryAggregation() {
        // if isGroupBy, Sum, Average, etc... isAggregate = true
        // TODO update to new LogicalSteps
        // TODO this release Aggregation won't be used
        mandatoryAggregation = false;

    }

    private void readLogicalWorkflow(LogicalStep initialLogicalStep) throws MongoQueryException, UnsupportedException {

        LogicalStep logicalStep = initialLogicalStep;

        // sortList = new ArrayList<Sort>();
        filterList = new ArrayList<Filter>();

        do {
            if (select != null)
                throw new MongoQueryException("select must be the last step");
            if (logicalStep instanceof Project) {
                if (projection == null)
                    projection = (Project) logicalStep;
                else
                    throw new UnsupportedException(" # Project > 1");
            } else if (logicalStep instanceof Filter) {
                filterList.add((Filter) logicalStep);
            } else if (logicalStep instanceof Limit) {
                if (limit == null)
                    limit = (Limit) logicalStep;
                else
                    throw new MongoQueryException(" # Limit > 1");
            } else if (logicalStep instanceof Select) {
                select = (Select) logicalStep;
            } else {
                throw new UnsupportedException("step unsupported" + logicalStep.getClass());
            }
        } while ((logicalStep = logicalStep.getNextStep()) != null);

        if (projection == null)
            throw new MongoQueryException("projection has not been found");
        if (select == null)
            throw new MongoQueryException("select has not been found");

    }

    public boolean isMandatoryAggregation() {
        return mandatoryAggregation;
    }

    private void buildQuery() {
        query = new ArrayList<DBObject>();

        if (mandatoryAggregation) {
            if (!filterList.isEmpty())
                query.add(buildFilter());
            // TODO It will be included in the following realease
        }

        else {
            query.add(buildFilter());
        }

    }

    private DBObject buildLimit() {
        LimitDBObjectBuilder limitDBObject = new LimitDBObjectBuilder(limit);
        return limitDBObject.build();
    }

    private DBObject buildProject() throws MongoQueryException {
        ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(mandatoryAggregation, projection, select);
        return projectDBObject.build();
    }

    private DBObject buildFilter() {

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(mandatoryAggregation);
        for (Filter f : filterList) {
            filterDBObjectBuilder.add(f);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Filter:" + filterDBObjectBuilder.build());
        }
        return filterDBObjectBuilder.build();
    }

    /**
     * Queries for objects in a collection
     * 
     * @throws MongoQueryException
     */

    public ResultSet executeQuery(MongoClient mongoClient) throws MongoQueryException {

        DB db = mongoClient.getDB(projection.getCatalogName());
        DBCollection coll = db.getCollection(projection.getTableName().getName());
        ResultSet resultSet = new ResultSet();
        // resultSet.setColumnMetadata(projection.getColumnList());// necesario??

        if (isMandatoryAggregation()) {

            // AggregationOptions aggOptions = AggregationOptions.builder()
            // .allowDiskUse(true)
            // .batchSize(size)
            // .maxTime(maxTime, timeUnit)
            // .outputMode(OutputMode.CURSOR) or INLINE
            // .build();
            // pipeline,aggOptions => dbcursor

            int stage = 1;
            for (DBObject aggregationStage : query) {
                logger.debug("Aggregate framework stage (" + (stage++) + ") : " + aggregationStage.toString());
            }

            AggregationOutput aggOutput = coll.aggregate(query);

            for (DBObject result : aggOutput.results()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("AggResult: " + result);
                }
                resultSet.add(createRowWithAlias(result));
            }

        } else {

            // ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(
            // false, projection);
            // DBObject fields = projectDBObject.build();
            DBObject fields = buildProject();

            // if !isCount
            // if !isDistinct
            // if !groupBy

            DBCursor cursor = coll.find(query.get(0), fields);
            DBObject rowDBObject;

            /*
             * // sort, skip and limit if (!sortList.isEmpty()) { // DBObject orderBy = new BasicDBObject();// asc o
             * desc, y varios // // sort posibles => // int sortType; // for (Sort sortElem : sortList) { // varios sort
             * // sortType = (sortElem.getType() == Sort.ASC) ? 1 : -1; // orderBy.put(sortElem.getField(), sortType);
             * // } DBObject orderBy = buildSort();
             * 
             * cursor = cursor.sort(orderBy); }
             */

            if (limit != null) {
                cursor = cursor.limit(limit.getLimit());
            }

            // iterate over the cursor
            try {
                while (cursor.hasNext()) { // Si no hay resultados => excepción..

                    rowDBObject = cursor.next();
                    if (logger.isDebugEnabled()) {
                        logger.debug("SResult: " + rowDBObject);
                    }
                    resultSet.add(createRowWithAlias(rowDBObject));
                }
            } catch (MongoException e) {
                // throw new ExecutionException("MongoException: "
                // + e.getMessage());
            } finally {
                cursor.close();
            }
        }

        resultSet.setColumnMetadata(createMetadata());
        return resultSet;

    }

    /**
     * This method creates a row from a Mongo result
     *
     * @param rowDBObject
     *            a bson containing the result.
     * @return the row.
     */
    private Row createRowWithAlias(DBObject rowDBObject) {
        Row row = new Row();
        Map<String, String> aliasMapping = select.getColumnMap();
        Set<String> fieldNames = aliasMapping.keySet();

        for (String field : fieldNames) {
            if (field.contains(".")) {
                String[] aField = field.split("\\.");
                field = aField[aField.length - 1];
            }
            Object value = rowDBObject.get(field);

            String qualifiedFieldName = QualifiedNames.getColumnQualifiedName(projection.getCatalogName(), projection
                            .getTableName().getName(), field);
            if (aliasMapping.containsKey(qualifiedFieldName)) {
                field = aliasMapping.get(qualifiedFieldName);
            }
            row.addCell(field, new Cell(value));
        }
        return row;
    }

    private List<ColumnMetadata> createMetadata() {
        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();
        for (String field : select.getColumnMap().keySet()) {
            if (field.contains(".")) {
                String[] aField = field.split("\\.");
                field = aField[aField.length - 1];
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getName(), field, select
                            .getTypeMap().get(field));
            columnMetadata.setColumnAlias(select.getColumnMap().get(field));
            retunColumnMetadata.add(columnMetadata);
        }
        return retunColumnMetadata;
    }

}
