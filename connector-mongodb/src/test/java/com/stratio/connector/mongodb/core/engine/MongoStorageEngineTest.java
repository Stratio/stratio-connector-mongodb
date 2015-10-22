/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.mongodb.core.engine;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.stratio.crossdata.common.metadata.DataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.StorageUtils;
import com.stratio.connector.mongodb.core.engine.metadata.UpdateDBObjectBuilder;
import com.stratio.connector.mongodb.core.engine.query.utils.FilterDBObjectBuilder;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

@PowerMockIgnore( {"javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DB.class, MongoClient.class, BasicDBObject.class, StorageUtils.class, MongoStorageEngine.class,
        FilterDBObjectBuilder.class })
public class MongoStorageEngineTest {

    private static final String CLUSTER_NAME = "clustername";
    private static final String COLLECTION_NAME = "coll_name";
    private static final String DB_NAME = "db_name";
    private static final String COLUMN_NAME = "row_name";
    private static final String OTHER_COLUMN_NAME = "OTHER_ROW_NAME";
    private static final String CELL_VALUE = "cell_value";
    private static final Object OTHER_CELL_VALUE = "othercellvalue";
    private static final ColumnType VARCHAR_COLUMN_TYPE = new ColumnType(DataType.VARCHAR);
    private static final ColumnType INT_COLUMN_TYPE = new ColumnType(DataType.INT);

//    @Mock
//    private MongoConnectionHandler connectionHandler;
//    @Mock
//    private Connection<MongoClient> connection;
//    @Mock
//    private MongoClient client;
//    @Mock
//    private DB database;
//    @Mock
//    private DBCollection collection;
//
//    private MongoStorageEngine mongoStorageEngine;

//    @Before
//    public void before() throws ExecutionException {
//        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
//        when(connection.getNativeConnection()).thenReturn(client);
//        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
//
//        when(client.getDB(DB_NAME)).thenReturn(database);
//
//    }

    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     *
     * @throws Exception
     */
    @Test
    public void testInsertWithoutPK() throws Exception {

        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(null);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        mongoStorageEngine.insert(tableMetadata, row, false, connection);

        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);
        verify(collection, times(1)).insert(doc);

    }

    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     *
     * @throws Exception
     */
    @Test
    public void testBatchInsertWithoutPK() throws Exception {

        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(null);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        BulkWriteOperation bulkWriteOp = mock(BulkWriteOperation.class);
        when(collection.initializeUnorderedBulkOperation()).thenReturn(bulkWriteOp);

        mongoStorageEngine.insert(tableMetadata,Arrays.asList(row, row), false, connection);

        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);

        verify(bulkWriteOp, times(2)).insert(doc);

    }

    @Test
    public void testInsertWithPK() throws Exception {

        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(COLUMN_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(CELL_VALUE);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        mongoStorageEngine.insert(tableMetadata,row, false, connection);

        DBObject pKeyDBObject = new BasicDBObject("_id", CELL_VALUE);
        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);

        verify(collection, times(1)).update(pKeyDBObject, new BasicDBObject("$set", doc), true, false);

    }

    @Test
    public void testBatchInsertWithPK() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(COLUMN_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(CELL_VALUE);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        BulkWriteOperation bulkWriteOp = mock(BulkWriteOperation.class);
        when(collection.initializeUnorderedBulkOperation()).thenReturn(bulkWriteOp);
        BulkWriteRequestBuilder bulkWriteRB = mock(BulkWriteRequestBuilder.class);
        when(bulkWriteOp.find(Matchers.any(DBObject.class))).thenReturn(bulkWriteRB);
        BulkUpdateRequestBuilder bulkWriteUpB = mock(BulkUpdateRequestBuilder.class);
        when(bulkWriteRB.upsert()).thenReturn(bulkWriteUpB);

        mongoStorageEngine.insert(tableMetadata,Arrays.asList(row, row), false, connection);

        DBObject pKeyDBObject = new BasicDBObject("_id", CELL_VALUE);
        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);

        verify(collection, times(1)).initializeUnorderedBulkOperation();
        verify(bulkWriteOp, times(2)).find(pKeyDBObject);
        verify(bulkWriteRB, times(2)).upsert();
        verify(bulkWriteUpB, times(2)).update(new BasicDBObject("$set", doc));
        verify(bulkWriteOp, times(1)).execute();

    }

    @Test
    public void testInsertIfNotExist() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(COLUMN_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(CELL_VALUE);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        BulkWriteOperation bulkWriteOp = mock(BulkWriteOperation.class);
        when(collection.initializeUnorderedBulkOperation()).thenReturn(bulkWriteOp);
        BulkWriteRequestBuilder bulkWriteRB = mock(BulkWriteRequestBuilder.class);
        when(bulkWriteOp.find(Matchers.any(DBObject.class))).thenReturn(bulkWriteRB);
        BulkUpdateRequestBuilder bulkWriteUpB = mock(BulkUpdateRequestBuilder.class);
        when(bulkWriteRB.upsert()).thenReturn(bulkWriteUpB);

        mongoStorageEngine.insert(tableMetadata,Arrays.asList(row, row), false, connection);

        DBObject pKeyDBObject = new BasicDBObject("_id", CELL_VALUE);
        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);

        verify(collection, times(1)).initializeUnorderedBulkOperation();
        verify(bulkWriteOp, times(2)).find(pKeyDBObject);
        verify(bulkWriteRB, times(2)).upsert();
        verify(bulkWriteUpB, times(2)).update(new BasicDBObject("$set", doc));
        verify(bulkWriteOp, times(1)).execute();
    }

    @Test
    public void testBatchInsertIfNotExist() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME, CLUSTER_NAME);
        tableMetaBuilder.addColumn(COLUMN_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_COLUMN_NAME, INT_COLUMN_TYPE)
                .withPartitionKey(COLUMN_NAME);

        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_COLUMN_NAME, new Cell(OTHER_CELL_VALUE));

        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(CELL_VALUE);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        BulkWriteOperation bulkWriteOp = mock(BulkWriteOperation.class);
        when(collection.initializeUnorderedBulkOperation()).thenReturn(bulkWriteOp);
        BulkWriteRequestBuilder bulkWriteRB = mock(BulkWriteRequestBuilder.class);
        when(bulkWriteOp.find(Matchers.any(DBObject.class))).thenReturn(bulkWriteRB);
        BulkUpdateRequestBuilder bulkWriteUpB = mock(BulkUpdateRequestBuilder.class);
        when(bulkWriteRB.upsert()).thenReturn(bulkWriteUpB);

        mongoStorageEngine.insert(tableMetadata,Arrays.asList(row, row), false, connection);

        DBObject pKeyDBObject = new BasicDBObject("_id", CELL_VALUE);
        BasicDBObject doc = new BasicDBObject(COLUMN_NAME, CELL_VALUE);
        doc.put(OTHER_COLUMN_NAME, OTHER_CELL_VALUE);

        verify(collection, times(1)).initializeUnorderedBulkOperation();
        verify(bulkWriteOp, times(2)).find(pKeyDBObject);
        verify(bulkWriteRB, times(2)).upsert();
        verify(bulkWriteUpB, times(2)).update(new BasicDBObject("$set", doc));
        verify(bulkWriteOp, times(1)).execute();

    }

    @Test
    public void updateTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);

        //DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        Relation rel1 = getBasicRelation(COLUMN_NAME, Operator.ASSIGN, CELL_VALUE);
        Relation rel2 = getBasicRelation(OTHER_COLUMN_NAME, Operator.ASSIGN, OTHER_CELL_VALUE);
        Collection<Relation> assignments = Arrays.asList(rel1, rel2);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_GT);

        Collection<Filter> whereClauses = Arrays.asList(new Filter(operations, getBasicRelation(
                COLUMN_NAME, Operator.GET, 20)));

        UpdateDBObjectBuilder updateDBObjectBuilder = mock(UpdateDBObjectBuilder.class);
        PowerMockito.whenNew(UpdateDBObjectBuilder.class).withNoArguments().thenReturn(updateDBObjectBuilder);

        mongoStorageEngine.update(tableName, assignments, whereClauses, connection);

        verify(updateDBObjectBuilder, times(2)).addUpdateRelation(Matchers.any(Selector.class),
                Matchers.any(Operator.class), Matchers.any(Selector.class));
        verify(updateDBObjectBuilder, times(1)).addUpdateRelation(rel1.getLeftTerm(), rel1.getOperator(),
                rel1.getRightTerm());
        verify(updateDBObjectBuilder, times(1)).addUpdateRelation(rel2.getLeftTerm(), rel2.getOperator(),
                rel2.getRightTerm());

        verify(collection, times(1)).update(Matchers.any(BasicDBObject.class), Matchers.any(BasicDBObject.class),
                Matchers.eq(false), Matchers.eq(true));

        PowerMockito.verifyPrivate(mongoStorageEngine, times(1)).invoke("buildFilter", whereClauses);

    }

    // TODO add 'black box' test => verify only the db interaction
    @Test
    public void updateInnerRelationTest() throws Exception {

        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);

        //DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        Relation rel1 = getBasicRelation(COLUMN_NAME, Operator.ASSIGN, CELL_VALUE);
        Relation rel2 = getBasicRelation(OTHER_COLUMN_NAME, Operator.ASSIGN, OTHER_CELL_VALUE);
        Collection<Relation> assignments = Arrays.asList(rel1, rel2);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_GT);

        Collection<Filter> whereClauses = Arrays.asList(new Filter(operations, getBasicRelation(
                COLUMN_NAME, Operator.GET, 20)));

        UpdateDBObjectBuilder updateDBObjectBuilder = mock(UpdateDBObjectBuilder.class);
        PowerMockito.whenNew(UpdateDBObjectBuilder.class).withNoArguments().thenReturn(updateDBObjectBuilder);

        Relation mockRelation = mock(Relation.class);
        Selector a = new BooleanSelector(true);
        Selector b = new IntegerSelector(1);
        Operator op = Operator.BETWEEN;
        when(mockRelation.getLeftTerm()).thenReturn(a);
        when(mockRelation.getRightTerm()).thenReturn(b);
        when(mockRelation.getOperator()).thenReturn(op);

        mongoStorageEngine.update(tableName, assignments, whereClauses, connection);

        verify(updateDBObjectBuilder, times(2)).addUpdateRelation(Matchers.any(Selector.class),
                Matchers.any(Operator.class), Matchers.any(Selector.class));
        verify(updateDBObjectBuilder, times(1)).addUpdateRelation(rel1.getLeftTerm(), rel1.getOperator(),
                rel1.getRightTerm());
        verify(updateDBObjectBuilder, times(1)).addUpdateRelation(rel2.getLeftTerm(), rel2.getOperator(),
                rel2.getRightTerm());

        verify(collection, times(1)).update(Matchers.any(BasicDBObject.class), Matchers.any(BasicDBObject.class),
                Matchers.eq(false), Matchers.eq(true));

        PowerMockito.verifyPrivate(mongoStorageEngine, times(1)).invoke("buildFilter", whereClauses);

    }

    // TODO allow different order
    @Test
    public void updateDecreaseTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);

        //DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        // Verify the object is build properly even although the command should fail when executing
        Relation rel1 = getBasicRelation(COLUMN_NAME, Operator.ASSIGN, CELL_VALUE);
        Relation rel2 = getBasicRelation(COLUMN_NAME, Operator.ASSIGN,
                getBasicRelation(COLUMN_NAME, Operator.SUBTRACT, 20l));
        Collection<Relation> assignments = Arrays.asList(rel1, rel2);

        mongoStorageEngine.update(tableName, assignments, null, connection);

        BasicDBObject updatedExpected = new BasicDBObject();
        updatedExpected.append("$set", new BasicDBObject(COLUMN_NAME, CELL_VALUE));
        updatedExpected.append("$inc", new BasicDBObject(COLUMN_NAME, -20l));

        verify(collection, times(1)).update(new BasicDBObject(), updatedExpected, false, true);

    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void buildNullFilterTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

//        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        Method method = mongoStorageEngine.getClass().getDeclaredMethod("buildFilter", Collection.class);
        method.setAccessible(true);

        DBObject filter = (DBObject) method.invoke(mongoStorageEngine, (Collection<Filter>) null);
        Assert.assertEquals("If there is no filters the query has to match all", new BasicDBObject(), filter);

    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void buildListFilterTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        //DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_GT);

        Collection<Filter> whereClauses = Arrays.asList(new Filter(operations, getBasicRelation(
                COLUMN_NAME, Operator.GET, 20)));

        Method method = mongoStorageEngine.getClass().getDeclaredMethod("buildFilter", Collection.class);
        method.setAccessible(true);

        FilterDBObjectBuilder filterBuilder = mock(FilterDBObjectBuilder.class);
        PowerMockito.whenNew(FilterDBObjectBuilder.class).withArguments(false, whereClauses).thenReturn(filterBuilder);

        DBObject filter = (DBObject) method.invoke(mongoStorageEngine, (Collection<Filter>) null);
        Assert.assertEquals("The built filter is not the expected", new BasicDBObject(), filter);

    }

    private Relation getBasicRelation(String column1, Operator assign, Object valueUpdated) {
        Selector leftSelector = new ColumnSelector(new ColumnName(DB_NAME, COLLECTION_NAME, column1));
        leftSelector.setAlias(column1);
        Selector rightSelector = null;
        if (valueUpdated instanceof Integer) {
            rightSelector = new IntegerSelector((int) valueUpdated);
        } else if (valueUpdated instanceof String) {
            rightSelector = new StringSelector((String) valueUpdated);
        } else if (valueUpdated instanceof Boolean) {
            rightSelector = new BooleanSelector((Boolean) valueUpdated);
        } else if (valueUpdated instanceof Relation) {
            rightSelector = new RelationSelector((Relation) valueUpdated);
        } else if (valueUpdated instanceof Long) {
            rightSelector = new IntegerSelector((int) (long) valueUpdated);
        }
        return new Relation(leftSelector, assign, rightSelector);
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void truncateTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        //DBCollection collection = mock(DBCollection.class);
        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);
        when(database.collectionExists(COLLECTION_NAME)).thenReturn(true);

        mongoStorageEngine.delete(tableName, null, connection);

        verify(collection, times(1)).remove(new BasicDBObject());
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void deleteBasicFilterTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        //DBCollection collection = mock(DBCollection.class);
        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);
        when(database.collectionExists(COLLECTION_NAME)).thenReturn(true);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_EQ);

        Collection<Filter> whereClauses = Arrays.asList(new Filter(operations, getBasicRelation(
                COLUMN_NAME, Operator.NOT_EQ, 20)));

        mongoStorageEngine.delete(tableName, whereClauses, connection);

        verify(collection, times(1)).remove(new BasicDBObject(COLUMN_NAME, new BasicDBObject("$ne", 20l)));
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void deleteCompoundFilterTest() throws Exception {
        MongoConnectionHandler connectionHandler = Mockito.mock(MongoConnectionHandler.class);
        Connection<MongoClient> connection = Mockito.mock(Connection.class);
        MongoClient client = Mockito.mock(MongoClient.class);
        DB database = Mockito.mock(DB.class);
        DBCollection collection = Mockito.mock(DBCollection.class);
        MongoStorageEngine mongoStorageEngine;

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = MongoStorageEngine.getInstance((MongoConnectionHandler) connectionHandler);
        when(client.getDB(DB_NAME)).thenReturn(database);

        //DBCollection collection = mock(DBCollection.class);
        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);
        TableName tableName = new TableName(DB_NAME, COLLECTION_NAME);
        when(database.collectionExists(COLLECTION_NAME)).thenReturn(true);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_EQ);

        Set<Operations> operations2 = new HashSet<>();
        operations2.add(Operations.FILTER_NON_INDEXED_LET);

        Collection<Filter> whereClauses = Arrays.asList(
                new Filter(operations, getBasicRelation(COLUMN_NAME, Operator.NOT_EQ, 20)),
                new Filter(operations2, getBasicRelation(OTHER_COLUMN_NAME, Operator.LET,
                        "b")));

        mongoStorageEngine.delete(tableName, whereClauses, connection);

        BasicDBObject filterA = new BasicDBObject(COLUMN_NAME, new BasicDBObject("$ne", 20l));
        BasicDBObject filterB = new BasicDBObject(new ColumnName(tableName, OTHER_COLUMN_NAME).getName(),
                new BasicDBObject("$lte", "b"));
        BasicDBList filterList = new BasicDBList();
        filterList.add(filterA);
        filterList.add(filterB);
        verify(collection, times(1)).remove(new BasicDBObject("$and", filterList));
    }
}
