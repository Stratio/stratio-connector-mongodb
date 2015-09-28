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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.*;

import com.stratio.crossdata.common.metadata.*;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.metadata.IndexMetadataBuilder;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.engine.metadata.AlterOptionsUtils;
import com.stratio.connector.mongodb.core.engine.metadata.DiscoverMetadataUtils;
import com.stratio.connector.mongodb.core.engine.metadata.IndexUtils;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
@PowerMockIgnore( {"javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MongoClient.class, Connection.class, IndexUtils.class, AlterOptionsUtils.class,
                MongoMetadataEngine.class, DiscoverMetadataUtils.class })
public class MongoMetadataEngineTest {

    private final static String CLUSTER_NAME = "clustername";
    private static final String DB_NAME = "catalog_name";
    private static final String DB_NAME_SEC = "catalog_sec";
    private static final String TABLE_NAME = "tablename";
    private static final String TABLE_NAME_SEC = "tablename_sec";
    private static final String INDEX_NAME = "indexname";
    private static final String COLUMN_NAME = "colname";
    private static final String COLUMN_NAME2 = "colname2";
   private static Map fielddWithType= new HashMap<String,String>();
    static {
        fielddWithType.put(COLUMN_NAME, "string");
    }

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
    public void before() throws Exception {
        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        mongoMetadataEngine = MongoMetadataEngine.getInstance(connectionHandler);

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

        mongoMetadataEngine.createTable(tableMetadata, connection);

        verify(tableMetadata, times(1)).getName();
        verify(tableMetadata, times(1)).getOptions();

    }

    @Test
    public void createTableWithoutNameTest() throws ExecutionException, UnsupportedException {

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(null);

        try {
            mongoMetadataEngine.createTable(tableMetadata, connection);
            fail("the table name is necessary");
        } catch (ExecutionException e) {
        }

        verify(tableMetadata, times(1)).getName();

    }

    @Test
    public void dropCatalogTest() throws ExecutionException, UnsupportedException {

        mongoMetadataEngine.dropCatalog(new CatalogName(DB_NAME), connection);
        verify(client, times(1)).dropDatabase(Matchers.anyString());

    }

    @Test
    public void dropCatalogExecutionExceptionTest() throws ExecutionException, UnsupportedException {

        Mockito.doThrow(new MongoException("exception")).when(client).dropDatabase(Matchers.anyString());
        try {
            mongoMetadataEngine.dropCatalog(new CatalogName(DB_NAME), connection);
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

        mongoMetadataEngine.dropTable(new TableName(DB_NAME, TABLE_NAME), connection);

        verify(collection, times(1)).drop();

    }

    @Test
    public void dropTableExecutionExceptionTest() throws ExecutionException, UnsupportedException {

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        Mockito.doThrow(new MongoException("exception")).when(collection).drop();

        try {
            mongoMetadataEngine.dropTable(new TableName(DB_NAME, TABLE_NAME), connection);
            fail("An execution exception must be thown");
        } catch (ExecutionException e) {

        }
        verify(collection, times(1)).drop();

    }

    @Test
    public void createIndexTest() throws ExecutionException, UnsupportedException {

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.DEFAULT);
        indexMetadataBuilder.addColumn(COLUMN_NAME, new ColumnType(DataType.VARCHAR));
        indexMetadataBuilder.addColumn(COLUMN_NAME2, new ColumnType(DataType.INT));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        // PowerMockito.mockStatic(IndexUtils.class);

        mongoMetadataEngine.createIndex(indexMetadata, connection);

        // PowerMockito.verifyStatic(Mockito.times(1));
        // IndexUtils.getIndexDBObject(indexMetadata);

        verify(client, times(1)).getDB(DB_NAME);
        verify(collection, times(1)).createIndex(new BasicDBObject(COLUMN_NAME, 1).append(COLUMN_NAME2, 1),
                        new BasicDBObject("name", INDEX_NAME));

    }

    @Test
    public void dropIndexTest() throws ExecutionException, UnsupportedException {

        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.DEFAULT);
        indexMetadataBuilder.addColumn(COLUMN_NAME, new ColumnType(DataType.VARCHAR));
        indexMetadataBuilder.addColumn(COLUMN_NAME2, new ColumnType(DataType.INT));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        PowerMockito.mockStatic(IndexUtils.class);

        mongoMetadataEngine.dropIndex(indexMetadata, connection);

        PowerMockito.verifyStatic(Mockito.never());
        IndexUtils.dropIndexWithDefaultName(indexMetadata, database);

        verify(client, times(1)).getDB(DB_NAME);
        verify(collection, times(1)).dropIndex(INDEX_NAME);

    }

    @Test(expected = ExecutionException.class)
    public void alterTableAlterOptionsTest() throws UnsupportedException, ExecutionException {

        TableName tableName = new TableName(DB_NAME, TABLE_NAME);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_COLUMN, null, mock(ColumnMetadata.class));
        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        mongoMetadataEngine.alterTable(tableName, alterOptions, connection);

    }

    @Test(expected = ExecutionException.class)
    public void alterTableAlterColumnTest() throws UnsupportedException, ExecutionException {

        TableName tableName = new TableName(DB_NAME, TABLE_NAME);
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_OPTIONS, null, mock(ColumnMetadata.class));
        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        mongoMetadataEngine.alterTable(tableName, alterOptions, connection);

    }

    @Test
    public void alterTableDropColumnTest() throws UnsupportedException, ExecutionException {

        TableName tableName = new TableName(DB_NAME, TABLE_NAME);
        ColumnMetadata columnMetadata = new ColumnMetadata(new ColumnName(DB_NAME, TABLE_NAME, COLUMN_NAME),
                        new Object[0], new ColumnType(DataType.INT));

        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, null, columnMetadata);
        DBCollection collection = mock(DBCollection.class);

        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);

        DBObject dropColumn = new BasicDBObject(COLUMN_NAME, new BasicDBObject("$unset", new BasicDBObject()));

        PowerMockito.mockStatic(AlterOptionsUtils.class);
        Mockito.when(AlterOptionsUtils.buildDropColumnDBObject(COLUMN_NAME)).thenReturn(dropColumn);

        mongoMetadataEngine.alterTable(tableName, alterOptions, connection);

        verify(collection, times(1)).updateMulti(any(BasicDBObject.class), Matchers.eq(dropColumn));

    }

    @Test(expected = UnsupportedException.class)
    public void alterCatalogTest() throws UnsupportedException, ExecutionException {
        mongoMetadataEngine.alterCatalog(new CatalogName(DB_NAME), null, connection);
    }

    @Test
    public void provideMetadataTest() throws ConnectorException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        MongoMetadataEngine spyMetadataEngine = PowerMockito.spy(MongoMetadataEngine.getInstance(connectionHandler));
        CatalogMetadata catalogMetadata1 = mock(CatalogMetadata.class);
        CatalogMetadata catalogMetadata2 = mock(CatalogMetadata.class);

        when(client.getDatabaseNames()).thenReturn(Arrays.asList(DB_NAME, DB_NAME_SEC));
        PowerMockito.doReturn(catalogMetadata1)
                        .doReturn(catalogMetadata2)
                        .when(spyMetadataEngine)
                        .provideCatalogMetadata(Matchers.any(CatalogName.class), Matchers.eq(clusterName),
                                        Matchers.eq(connection));

        List<CatalogMetadata> providedMetadata = spyMetadataEngine.provideMetadata(clusterName, connection);

        Assert.assertEquals(providedMetadata.size(), 2);
        Assert.assertEquals(providedMetadata.get(0), catalogMetadata1);
        Assert.assertEquals(providedMetadata.get(1), catalogMetadata2);

    }

    @Test
    public void provideCatalogMetadataTest() throws ConnectorException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        MongoMetadataEngine spyMetadataEngine = PowerMockito.spy(MongoMetadataEngine.getInstance(connectionHandler));

        TableMetadata tableMetadata1 = mock(TableMetadata.class);
        TableMetadata tableMetadata2 = mock(TableMetadata.class);

        TableName tableName1 = new TableName(DB_NAME, TABLE_NAME);
        TableName tableName2 = new TableName(DB_NAME, TABLE_NAME_SEC);

        when(tableMetadata1.getName()).thenReturn(tableName1);
        when(tableMetadata2.getName()).thenReturn(tableName2);

        Set<String> set = new TreeSet<>();
        set.add(TABLE_NAME);
        set.add(TABLE_NAME_SEC);
        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollectionNames()).thenReturn(set);
        PowerMockito.doReturn(tableMetadata1).when(spyMetadataEngine)
                        .provideTableMetadata(new TableName(DB_NAME, TABLE_NAME), clusterName, connection);
        PowerMockito.doReturn(tableMetadata2).when(spyMetadataEngine)
                        .provideTableMetadata(new TableName(DB_NAME, TABLE_NAME_SEC), clusterName, connection);

        CatalogMetadata providedCatalogMetadata = spyMetadataEngine.provideCatalogMetadata(new CatalogName(DB_NAME),
                        clusterName, connection);

        Assert.assertEquals(providedCatalogMetadata.getName().getName(), DB_NAME);
        Assert.assertEquals(providedCatalogMetadata.getTables().size(), 2);
        Assert.assertEquals(providedCatalogMetadata.getTables().get(tableName1), tableMetadata1);
        Assert.assertEquals(providedCatalogMetadata.getTables().get(tableName2), tableMetadata2);

    }

    @Test
    public void provideTableMetadataTest() throws ConnectorException {

        DBCollection collection = mock(DBCollection.class);
        when(client.getDB(DB_NAME)).thenReturn(database);
        when(database.getCollection(TABLE_NAME)).thenReturn(collection);
        PowerMockito.mockStatic(DiscoverMetadataUtils.class);

        when(DiscoverMetadataUtils.discoverFieldsWithType(collection,"1")).thenReturn(fielddWithType);
        when(DiscoverMetadataUtils.discoverIndexes(collection)).thenReturn(
                getIndexMetadata(COLUMN_NAME2, IndexType.CUSTOM));

        TableMetadata tableMetadataProvided = mongoMetadataEngine.provideTableMetadata(new TableName(DB_NAME,
                        TABLE_NAME), new ClusterName(CLUSTER_NAME), connection);

        // names
        Assert.assertEquals("The table name is not the expected", TABLE_NAME, tableMetadataProvided.getName().getName());
        Assert.assertEquals("The cluster name is not the expected", CLUSTER_NAME, tableMetadataProvided.getClusterRef()
                        .getName());
        Assert.assertEquals("The catalog name is not the expected", DB_NAME, tableMetadataProvided.getName()
                        .getCatalogName().getName());
        // pKey
        Assert.assertEquals("The primary key must have only one field", 1, tableMetadataProvided.getPrimaryKey().size());
        Assert.assertEquals("The primary key must be the first field", tableMetadataProvided.getColumns().entrySet().iterator().next().getKey().getName(), tableMetadataProvided.getPrimaryKey().get(0)
                        .getName());

        // columns
        Assert.assertEquals("The columns must have one column", 1, tableMetadataProvided.getColumns().size());
        Assert.assertTrue("The column found in existing bson is not the expected", tableMetadataProvided.getColumns()
                        .keySet().contains(new ColumnName(DB_NAME, TABLE_NAME, COLUMN_NAME)));
        Assert.assertTrue("The column found in existing index is not the expected",

        tableMetadataProvided.getColumns().keySet().contains(new ColumnName(DB_NAME, TABLE_NAME, COLUMN_NAME)));
        // index
        Assert.assertEquals("There must be a single index", 1, tableMetadataProvided.getIndexes().size());
        Assert.assertTrue("The index name is not the expected",
                        tableMetadataProvided.getIndexes().containsKey(new IndexName(DB_NAME, TABLE_NAME, INDEX_NAME)));
        IndexMetadata indexMetadata = tableMetadataProvided.getIndexes().get(
                        new IndexName(DB_NAME, TABLE_NAME, INDEX_NAME));
        Assert.assertEquals("The index type is not the expected", IndexType.CUSTOM, indexMetadata.getType());
        Assert.assertEquals("There must be a single column", 1, indexMetadata.getColumns().size());
        Assert.assertTrue("There index column is not the expected",
                        indexMetadata.getColumns().containsKey(new ColumnName(DB_NAME, TABLE_NAME, COLUMN_NAME2)));
    }

    private List<IndexMetadata> getIndexMetadata(String columnName, IndexType indexType) {
        IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME, indexType);
        return Arrays.asList(indexMetadataBuilder.addColumn(columnName, new ColumnType(DataType.INT)).build());
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
