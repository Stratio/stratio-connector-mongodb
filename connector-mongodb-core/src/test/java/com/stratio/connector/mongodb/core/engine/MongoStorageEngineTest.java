/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.mongodb.core.engine;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.StorageUtils;
import com.stratio.connector.mongodb.testutils.TableMetadataBuilder;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DB.class, MongoClient.class, BasicDBObject.class, StorageUtils.class, MongoStorageEngine.class })
public class MongoStorageEngineTest {

    private static final String CLUSTER_NAME = "clustername";
    private static final String COLLECTION_NAME = "coll_name";
    private static final String DB_NAME = "db_name";
    private static final String ROW_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME";
    private static final String CELL_VALUE = "cell_value";
    private static final Object OTHER_CELL_VALUE = "othercellvalue";
    private static final Integer INTEGER_CELL_VALUE = new Integer(5);
    private static final ColumnType VARCHAR_COLUMN_TYPE = ColumnType.VARCHAR;
    private static final ColumnType INT_COLUMN_TYPE = ColumnType.INT;

    @Mock
    private MongoConnectionHandler connectionHandler;
    @Mock
    private Connection<MongoClient> connection;
    @Mock
    private MongoClient client;
    @Mock
    private DB database;
    @Mock
    private DBCollection collection;

    private MongoStorageEngine mongoStorageEngine;

    @Before
    public void before() throws HandlerConnectionException {
        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoStorageEngine = new MongoStorageEngine(connectionHandler);

        when(client.getDB(DB_NAME)).thenReturn(database);

    }

    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     * 
     * @throws Exception
     */
    @Test
    public void testInsertWithoutPK() throws Exception {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        tableMetaBuilder.addColumn(ROW_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_ROW_NAME, INT_COLUMN_TYPE);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(ROW_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_ROW_NAME, new Cell(OTHER_CELL_VALUE));

        BasicDBObject doc = mock(BasicDBObject.class);
        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(null);
        PowerMockito.whenNew(BasicDBObject.class).withNoArguments().thenReturn(doc);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        mongoStorageEngine.insert(clusterName, tableMetadata, row);

        verify(doc, times(2)).put(Matchers.anyString(), Matchers.anyObject());
        verify(collection, times(1)).insert(Matchers.any(BasicDBObject.class));

    }

    @Test
    public void testInsertWithPK() throws Exception {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);
        TableMetadataBuilder tableMetaBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        tableMetaBuilder.addColumn(ROW_NAME, VARCHAR_COLUMN_TYPE).addColumn(OTHER_ROW_NAME, INT_COLUMN_TYPE);
        tableMetaBuilder.withPartitionKey(ROW_NAME);
        TableMetadata tableMetadata = tableMetaBuilder.build();
        Row row = new Row();
        row.addCell(ROW_NAME, new Cell(CELL_VALUE));
        row.addCell(OTHER_ROW_NAME, new Cell(OTHER_CELL_VALUE));

        BasicDBObject doc = mock(BasicDBObject.class);
        PowerMockito.mockStatic(StorageUtils.class);
        when(StorageUtils.buildPK(tableMetadata, row)).thenReturn(CELL_VALUE);
        PowerMockito.whenNew(BasicDBObject.class).withNoArguments().thenReturn(doc);
        when(database.getCollection(COLLECTION_NAME)).thenReturn(collection);

        mongoStorageEngine.insert(clusterName, tableMetadata, row);

        verify(collection, times(1)).update(Matchers.any(BasicDBObject.class), Matchers.any(BasicDBObject.class),
                        Matchers.eq(true), Matchers.eq(false));

    }

}
