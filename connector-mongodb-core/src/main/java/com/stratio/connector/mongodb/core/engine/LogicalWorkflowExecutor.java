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
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
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
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnType;

public class LogicalWorkflowExecutor {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Project projection = null;
    /**
     * The list of filters without including full-text filters
     */
    private ArrayList<Filter> filterList = null;
    private Filter textFilter = null;
    private Limit limit = null;
    private Select select = null;
    /**
     * Whether the aggregation framework is compulsory for logicalworkflow
     */
    private boolean aggregationRequired;

    private List<DBObject> query = null;

    public LogicalWorkflowExecutor(LogicalStep initialProject) throws MongoQueryException, UnsupportedException {

        readLogicalWorkflow(initialProject);
        aggregationRequired();
        buildQuery();

    }

    private void aggregationRequired() {
        // Aggregation features will be included in the next release
        aggregationRequired = false;

    }

    private void readLogicalWorkflow(LogicalStep initialProject) throws MongoQueryException, UnsupportedException {

        LogicalStep logicalStep = initialProject;

        filterList = new ArrayList<Filter>();

        do {
            if (select != null)
                throw new MongoValidationException("Select must be the last step");
            if (logicalStep instanceof Project) {
                if (projection == null)
                    projection = (Project) logicalStep;
                else
                    throw new MongoValidationException(" # Project > 1");
            } else if (logicalStep instanceof Filter) {
                Filter step = (Filter) logicalStep;
                if (Operator.MATCH == step.getRelation().getOperator()) {
                    throw new MongoValidationException("Full-text queries not yet supported");
                } else {
                    filterList.add(step);
                }

            } else if (logicalStep instanceof Limit) {
                if (limit == null)
                    limit = (Limit) logicalStep;
                else
                    throw new MongoValidationException(" # Limit > 1");
            } else if (logicalStep instanceof Select) {
                select = (Select) logicalStep;
            } else {
                throw new MongoValidationException("Step unsupported" + logicalStep.getClass());
            }
        } while ((logicalStep = logicalStep.getNextStep()) != null);

        if (projection == null)
            throw new MongoValidationException("Projection has not been found in the logical workflow");
        if (select == null)
            throw new MongoValidationException("Select has not been found in the logical workflow");

    }

    private boolean isAggregationRequired() {
        return aggregationRequired;
    }

    private void buildQuery() throws MongoValidationException {
        query = new ArrayList<DBObject>();

        if (isAggregationRequired()) {
            // TODO It will be included in the following release
            if (!filterList.isEmpty())
                query.add(buildFilter());
            query.add(buildLimit());
        }

        else {
            query.add(buildFilter());
        }

    }

    private DBObject buildLimit() {
        LimitDBObjectBuilder limitDBObject = new LimitDBObjectBuilder(limit);
        return limitDBObject.build();
    }

    private DBObject buildProject() throws MongoValidationException {
        ProjectDBObjectBuilder projectDBObject = new ProjectDBObjectBuilder(aggregationRequired, select);
        return projectDBObject.build();
    }

    private DBObject buildFilter() throws MongoValidationException {

        FilterDBObjectBuilder filterDBObjectBuilder = new FilterDBObjectBuilder(aggregationRequired);

        for (Filter f : filterList) {
            filterDBObjectBuilder.add(f);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Filter:" + filterDBObjectBuilder.build());
        }
        return filterDBObjectBuilder.build();
    }

    /**
     * Execute the query
     * 
     * @param mongoClient
     * @return the result set
     * @throws MongoQueryException
     * @throws MongoValidationException
     */
    public ResultSet executeQuery(MongoClient mongoClient) throws MongoQueryException, MongoValidationException {

        DB db = mongoClient.getDB(projection.getCatalogName());
        DBCollection coll = db.getCollection(projection.getTableName().getName());
        ResultSet resultSet = new ResultSet();

        if (isAggregationRequired()) {

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

            DBCursor cursor = coll.find(query.get(0), buildProject());

            if (limit != null) {
                cursor = cursor.limit(limit.getLimit());
            }

            DBObject rowDBObject;
            try {
                while (cursor.hasNext()) { // Si no hay resultados => excepción..

                    rowDBObject = cursor.next();
                    if (logger.isDebugEnabled()) {
                        logger.debug("SResult: " + rowDBObject);
                    }
                    resultSet.add(createRowWithAlias(rowDBObject));
                }
            } catch (MongoException e) {
                throw new MongoQueryException(e.getMessage(), e);
            } finally {
                cursor.close();
            }

        }

        if (!resultSet.isEmpty())
            resultSet.setColumnMetadata(createMetadata());

        return resultSet;

    }

    /**
     * This method creates a row from a Mongo result. If there is no result a null value is inserted
     *
     * @param rowDBObject
     *            a bson containing the result.
     * @return the row.
     */
    private Row createRowWithAlias(DBObject rowDBObject) {
        Row row = new Row();
        Map<ColumnName, String> aliasMapping = select.getColumnMap();
        Set<ColumnName> fieldNames = aliasMapping.keySet();
        String field;
        for (ColumnName colName : fieldNames) {
            field = colName.getName();
            Object value = rowDBObject.get(field);

            if (aliasMapping.containsKey(colName))
                field = aliasMapping.get(colName);

            row.addCell(field, new Cell(value));
        }
        return row;
    }

    private List<ColumnMetadata> createMetadata() {
        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();
        for (ColumnName colName : select.getColumnMap().keySet()) {

            String field = colName.getName();
            ColumnType colType = select.getTypeMap().get(colName.getQualifiedName());

            colType = updateColumnType(colType);

            ColumnMetadata columnMetadata = new ColumnMetadata(projection.getTableName().getName(), field, colType);
            columnMetadata.setColumnAlias(select.getColumnMap().get(colName));

            retunColumnMetadata.add(columnMetadata);
        }
        return retunColumnMetadata;
    }

    private ColumnType updateColumnType(ColumnType colType) {
        String dbType;
        switch (colType) {
        case FLOAT:
            // TODO check the meaning of DBType
            dbType = colType.getODBCType();
            colType.setDBMapping(dbType, Double.class);
            break;
        case SET:
        case LIST:
            // TODO Set? BasicDBList extends ArrayList<Object>. how to define??
            dbType = colType.getODBCType();
            colType.setDBMapping(dbType, List.class);
            colType.setDBCollectionType(updateColumnType(colType.getDBInnerType()));
            break;
        case MAP:
            // TODO DBObject?
            dbType = colType.getODBCType();
            colType.setDBMapping(dbType, Map.class);
            colType.setDBMapType((updateColumnType(colType.getDBInnerType())),
                            updateColumnType(colType.getDBInnerValueType()));
            break;

        case NATIVE:
            // TODO Case not supported
            // // TODO check? and setOdbcType??
            // dbType = colType.getDbType();
            // // TODO check the row??
            // if (NativeTypes.DATE.equals(colType.getDbType()))
            // colType.setDBMapping(dbType, Date.class);
            break;
        case BIGINT:
        case BOOLEAN:
        case DOUBLE:
        case INT:
        case TEXT:
        case VARCHAR:
        default:
            break;
        }
        return colType;

    }
}
