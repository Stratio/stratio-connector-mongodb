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

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * @author darroyo
 *
 */
public class MongoMetadataEngine extends CommonsMetadataEngine {

    private transient MongoConnectionHandler connectionHandler;
    // TODO meta name?
    static public final String SHARDING_ENABLED = "enable_sharding";

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * @param connectionHandler
     *            the connector handler
     */
    public MongoMetadataEngine(MongoConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * Create a catalog in MongoDB.
     *
     * @param targetCluster
     *            the cluster to be created.
     * @param indexMetaData
     *            the index configuration.
     * @throws UnsupportedException
     *             if any operation is not supported.
     * @throws ExecutionException
     *             if an error occur.
     */
    @Override
    public void createCatalog(CatalogMetadata catalogMetadata, Connection connection) throws ExecutionException,
                    UnsupportedException {

        MongoClient mongoClient = (MongoClient) connection.getNativeConnection();
        Map<TableName, TableMetadata> tables = catalogMetadata.getTables();
        boolean isCatalogSharded = false;
        Iterator<TableName> keyIterator = tables.keySet().iterator();

        // Sharding operations
        while (keyIterator.hasNext() && !isCatalogSharded) {
            isCatalogSharded = collectionIsSharded(tables.get(keyIterator.next()));

        }
        if (isCatalogSharded) {
            enableSharding(mongoClient, catalogMetadata.getName().getName());
            for (TableMetadata tableMetadata : tables.values()) {
                shardCollection(mongoClient, tableMetadata, isCatalogSharded);
            }
        }

        // Index operations
        for (TableMetadata tableMetadata : tables.values()) {
            if (tableMetadata.getIndexes() != null) {
                for (IndexMetadata indexMetadata : tableMetadata.getIndexes().values()) {
                    createIndex(indexMetadata, connection);
                }
            }
        }

    }

    @Override
    public void createTable(TableMetadata tableMetadata, Connection connection) throws ExecutionException,
                    UnsupportedException {
        shardCollection((MongoClient) connection.getNativeConnection(), tableMetadata, false);
    }

    /**
     * Shard a collection if the table option "enable_sharding" is true
     * 
     * @param mongoClient
     * @param tableMetadata
     * @param shardingEnabled
     *            whether sharding has been enabled previously for the catalog or not
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private void shardCollection(MongoClient mongoClient, TableMetadata tableMetadata, boolean shardingEnabled)
                    throws ExecutionException, UnsupportedException {

        final String catalogName = tableMetadata.getName().getCatalogName().getName();

        if (collectionIsSharded(tableMetadata)) {

            if (!shardingEnabled)
                enableSharding(mongoClient, catalogName);

            DB catalog;
            catalog = mongoClient.getDB("admin");

            // TODO tableMetadata.getOptions();
            // TODO shardKey hashed=> ("date", "hashed"); o multicampo ascendente o descendente

            // create a shardKey
            // TODO required a shardKey con el tipo de índice
            /*
             * ShardKey shardKey = tableMetadata.getShardKey(); for( shardKey.getShardField()){
             * shardField.getIndexType() }
             */
            // TODO hashed key by default
            // final DBObject shardKey = new BasicDBObject("_id", "hashed");
            // TODO _id key by default
            final DBObject shardKey = new BasicDBObject("_id", 1);

            // shard the collection with the key
            final DBObject cmd = new BasicDBObject("shardCollection", catalogName + "."
                            + tableMetadata.getName().getName());
            cmd.put("key", shardKey);

            CommandResult result = catalog.command(cmd);
            if (!result.ok()) {
                logger.error("Command Error:" + result.getErrorMessage());
                throw new ExecutionException(result.getErrorMessage());
            }
        }

    }

    /**
     * @param tableMetadata
     * @return true if sharding is requiered
     */
    private boolean collectionIsSharded(TableMetadata tableMetadata) {

        Selector selectorSharded = null;

        /*
         * boolean isSharded = false; // TODO META option if collection is sharded for (Selector sel :
         * tableMetadata.getOptions().keySet()) { if (sel instanceof StringSelector) { if (((StringSelector)
         * sel).getValue().equals(SHARDING_ENABLED)) { isSharded = true; } } } return isSharded;
         */

        if ((selectorSharded = tableMetadata.getOptions().get(new StringSelector(SHARDING_ENABLED))) != null) {
            return ((BooleanSelector) selectorSharded).getValue();
            // TODO

        } else
            return false;

    }

    private void enableSharding(MongoClient mongoClient, String catalogName) throws ExecutionException {

        DB catalog;

        catalog = mongoClient.getDB("admin");

        CommandResult result = catalog.command(new BasicDBObject("enableSharding", catalogName));
        if (!result.ok()) {
            logger.error("Command Error:" + result.getErrorMessage());
            if (!result.getErrorMessage().equals("already enabled"))
                throw new ExecutionException(result.getErrorMessage());
        }
    }

    /**
     * Drop a database in MongoDB.
     *
     * @param targetCluster
     *            the cluster to be dropped.
     * @param name
     *            the database name.
     */
    @Override
    public void dropCatalog(CatalogName name, Connection connection) throws ExecutionException {
        ((MongoClient) connection.getNativeConnection()).dropDatabase(name.getName());
    }

    /**
     * Drop a collection in MongoDB.
     *
     * @param targetCluster
     *            the cluster to be dropped.
     * @param name
     *            the database name.
     */
    @Override
    public void dropTable(TableName name, Connection connection) throws ExecutionException {

        DB db = ((MongoClient) connection.getNativeConnection()).getDB(name.getCatalogName().getName());

        db.getCollection(name.getName()).drop();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IMetadataEngine#createIndex(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    public void createIndex(IndexMetadata indexMetadata, Connection connection) throws ExecutionException,
                    UnsupportedException {

        DB db = ((MongoClient) connection.getNativeConnection()).getDB(indexMetadata.getName().getTableName()
                        .getCatalogName().getName());

        DBObject indexDBObject = new BasicDBObject();
        DBObject indexOptionsDBObject = null;
        String indexName = indexMetadata.getName().getName();

        if (indexMetadata.getType() == IndexType.DEFAULT) {

            for (ColumnMetadata columnMeta : indexMetadata.getColumns()) {
                indexDBObject.put(columnMeta.getName().getName(), 1);
            }

        } else if (indexMetadata.getType() == IndexType.FULL_TEXT) {
            for (ColumnMetadata columnMeta : indexMetadata.getColumns()) {
                indexDBObject.put(columnMeta.getName().getName(), "text");
            }

        } else
            throw new UnsupportedException("index type " + indexMetadata.getType().toString() + " is not supported");

        if (indexName != null && !indexName.trim().isEmpty()) {
            indexOptionsDBObject = new BasicDBObject("name", indexName);
            db.getCollection(indexMetadata.getName().getTableName().getName()).createIndex(indexDBObject,
                            indexOptionsDBObject);
        } else
            db.getCollection(indexMetadata.getName().getTableName().getName()).createIndex(indexDBObject);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IMetadataEngine#dropIndex(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    public void dropIndex(IndexMetadata indexMetadata, Connection connection) throws ExecutionException,
                    UnsupportedException {
        DB db = ((MongoClient) connection.getNativeConnection()).getDB(indexMetadata.getName().getTableName()
                        .getCatalogName().getName());

        String indexName = null;
        if (indexMetadata.getName() != null)
            indexName = indexMetadata.getName().getName();

        if (indexName != null) {

            db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(indexName);

        } else {

            if (indexMetadata.getType() == IndexType.DEFAULT) {
                DBObject indexDBObject = new BasicDBObject();
                for (ColumnMetadata columnMeta : indexMetadata.getColumns()) {
                    indexDBObject.put(columnMeta.getName().getName(), 1);
                }
                db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(indexDBObject);

            } else if (indexMetadata.getType() == IndexType.FULL_TEXT) {

                String defaultTextIndexName = "";

                int colNumber = 0;
                int columnSize = indexMetadata.getColumns().size();

                for (ColumnMetadata columnMeta : indexMetadata.getColumns()) {
                    defaultTextIndexName += columnMeta.getName().getName() + "_";

                    if (++colNumber != columnSize)
                        defaultTextIndexName += "_";

                }

                db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(defaultTextIndexName);

            } else
                throw new UnsupportedException("index type " + indexMetadata.getType().toString() + " is not supported");

        }

    }

}
