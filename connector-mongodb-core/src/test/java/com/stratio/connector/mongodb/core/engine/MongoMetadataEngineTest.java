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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.IndexUtils;
import com.stratio.connector.mongodb.testutils.IndexMetadataBuilder;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MongoClient.class, Connection.class, IndexUtils.class })
public class MongoMetadataEngineTest {

    private final static String CLUSTER_NAME = "clustername";
    private static final String DB_NAME = "catalog_name";
    private static final String TABLE_NAME = "tablename";
    private final String INDEX_NAME = "indexname";
    private final String COLUMN_NAME = "colname";
    private final String COLUMN_NAME2 = "colname2";

    MongoMetadataEngine mongoMetadataEngine;
    @Mock
    MongoConnectionHandler connectionHandler;
    @Mock
    Connection<MongoClient> connection;
    @Mock
    MongoClient client;
    @Mock
    DB database;

    @Before
    public void before() throws HandlerConnectionException, Exception {
        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoMetadataEngine = new MongoMetadataEngine(connectionHandler);

    }

    @Test(expected = UnsupportedException.class)
    public void createCatalogTest() throws ExecutionException, UnsupportedException {
        mongoMetadataEngine.createCatalog(Matchers.any(CatalogMetadata.class), connection);
    }

    @Test
    public void createTableTest() throws UnsupportedException, ExecutionException {
        // TODO inlcude table with sharded collections

        TableName tableName = new TableName(DB_NAME, TABLE_NAME);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(tableName);

        mongoMetadataEngine.createTable(new ClusterName(CLUSTER_NAME), tableMetadata);

        verify(tableMetadata, times(1)).getName();
        verify(tableMetadata, times(1)).getOptions();

    }

    @Test
    public void createTableWithoutNameTest() throws ExecutionException {

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(null);

        try {
            mongoMetadataEngine.createTable(new ClusterName(CLUSTER_NAME), tableMetadata);
            fail("the table name is necessary");
        } catch (UnsupportedException e) {
        }

        verify(tableMetadata, times(1)).getName();

    }

    @Test
    public void dropCatalogTest() throws ExecutionException, UnsupportedException {

        mongoMetadataEngine.dropCatalog(new ClusterName(CLUSTER_NAME), new CatalogName(DB_NAME));
        verify(client, times(1)).dropDatabase(Matchers.anyString());

    }

    @Test
    public void dropCatalogExecutionExceptionTest() throws ExecutionException, UnsupportedException {

        Mockito.doThrow(new MongoException("exception")).when(client).dropDatabase(Matchers.anyString());
        try {
            mongoMetadataEngine.dropCatalog(new ClusterName(CLUSTER_NAME), new CatalogName(DB_NAME));
            fail("An execution exception must be thown");
        } catch (ExecutionException e) {
        }

        verify(client, times(1)).dropDatabase(Matchers.anyString());

    }

    @Test
    public void dropTableTest() throws ExecutionException, UnsupportedException {

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        mongoMetadataEngine.dropTable(new ClusterName(CLUSTER_NAME), new TableName(DB_NAME, TABLE_NAME));

        verify(collection, times(1)).drop();

    }

    @Test
    public void dropTableExecutionExceptionTest() throws ExecutionException, UnsupportedException {

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        Mockito.doThrow(new MongoException("exception")).when(collection).drop();

        try {
            mongoMetadataEngine.dropTable(new ClusterName(CLUSTER_NAME), new TableName(DB_NAME, TABLE_NAME));
            fail("An execution exception must be thown");
        } catch (ExecutionException e) {

        }
        verify(collection, times(1)).drop();

    }

    @Test
    public void createIndexTest() throws ExecutionException, UnsupportedException {

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.DEFAULT);
        indexMetadataBuilder.addColumn(COLUMN_NAME, ColumnType.VARCHAR);
        indexMetadataBuilder.addColumn(COLUMN_NAME2, ColumnType.INT);
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        PowerMockito.mockStatic(IndexUtils.class);

        mongoMetadataEngine.createIndex(new ClusterName(CLUSTER_NAME), indexMetadata);

        PowerMockito.verifyStatic(Mockito.times(1));
        IndexUtils.getIndexDBObject(indexMetadata);

        PowerMockito.verifyStatic(Mockito.times(1));
        IndexUtils.getIndexDBObject(indexMetadata);

        verify(client, times(1)).getDB(DB_NAME);
        verify(collection, times(1)).createIndex(any(DBObject.class), any(DBObject.class));

    }

    @Test
    public void dropIndexTest() throws ExecutionException, UnsupportedException {

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.DEFAULT);
        indexMetadataBuilder.addColumn(COLUMN_NAME, ColumnType.VARCHAR);
        indexMetadataBuilder.addColumn(COLUMN_NAME2, ColumnType.INT);
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        PowerMockito.mockStatic(IndexUtils.class);

        mongoMetadataEngine.dropIndex(new ClusterName(CLUSTER_NAME), indexMetadata);

        PowerMockito.verifyStatic(Mockito.never());
        IndexUtils.dropIndexWithDefaultName(indexMetadata, database);

        verify(client, times(1)).getDB(DB_NAME);
        verify(collection, times(1)).dropIndex(INDEX_NAME);

    }

    // TODO Options => IndexUtils
    // Map<Selector, Selector> options = new HashMap<Selector, Selector>();
    // StringSelector optSelector = new StringSelector("index_type");
    // StringSelector optValue = new StringSelector("compound");
    // options.put(optSelector, optValue);
    //
    // StringSelector optSelector2 = new StringSelector("compound_fields");
    // StringSelector optValue2 = new StringSelector("field1:asc, field2:desc");
    // options.put(optSelector2, optValue2);
    //
    // StringSelector optSelector3 = new StringSelector("unique");
    // optSelector3.setAlias("unique");
    // BooleanSelector optValue3 = new BooleanSelector(true);
    // options.put(optSelector3, optValue3);
    //
    // IndexMetadata indexMetadata = new IndexMetadata(new IndexName(tableName, INDEX_NAME), columns,
    // IndexType.CUSTOM, options);

}
