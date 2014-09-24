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
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * @author darroyo
 *
 */
public class MongoMetadataEngine implements IMetadataEngine {

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
        this.connectionHandler = connectionHandler;
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
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws ExecutionException,
                    UnsupportedException {

        Map<TableName, TableMetadata> tables = catalogMetadata.getTables();
        boolean isCatalogSharded = false;
        Iterator<TableName> keyIterator = tables.keySet().iterator();

        // Sharding operations
        while (keyIterator.hasNext() && !isCatalogSharded) {
            isCatalogSharded = collectionIsSharded(tables.get(keyIterator.next()));

        }
        if (isCatalogSharded) {
            enableSharding(targetCluster, catalogMetadata.getName().getName());
            for (TableMetadata tableMetadata : tables.values()) {
                shardCollection(targetCluster, tableMetadata, isCatalogSharded);
            }
        }

        // Index operations
        for (TableMetadata tableMetadata : tables.values()) {
            if (tableMetadata.getIndexes() != null) {
                for (IndexMetadata indexMetadata : tableMetadata.getIndexes().values()) {
                    createIndex(targetCluster, indexMetadata);
                }
            }
        }

    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws ExecutionException,
                    UnsupportedException {
        shardCollection(targetCluster, tableMetadata, false);
    }

    /**
     * Shard a collection if the table option "enable_sharding" is true
     * 
     * @param targetCluster
     * @param tableMetadata
     * @param shardingEnabled
     *            whether sharding has been enabled previously for the catalog or not
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private void shardCollection(ClusterName targetCluster, TableMetadata tableMetadata, boolean shardingEnabled)
                    throws ExecutionException, UnsupportedException {

        final String catalogName = tableMetadata.getName().getCatalogName().getName();

        if (collectionIsSharded(tableMetadata)) {

            if (!shardingEnabled)
                enableSharding(targetCluster, catalogName);

            DB catalog;
            try {
                catalog = recoveredClient(targetCluster).getDB("admin");
            } catch (HandlerConnectionException e) {
                throw new ExecutionException("admin cannot be recovered: " + targetCluster.getName(), e);
            }

            // TODO tableMetadata.getOptions();
            // TODO shardKey hashed=> ("date", "hashed"); o multicampo ascendente o descendente

            // create a shardKey
            // TODO required a shardKey con el tipo de índice
            /*
             * ShardKey shardKey = tableMetadata.getShardKey(); for( shardKey.getShardField()){
             * shardField.getIndexType() }
             */
            // TODO hashed key by default
            final DBObject shardKey = new BasicDBObject("_id", "hashed");

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
        boolean isSharded = false;
        // TODO META option if collection is sharded
        boolean a = false;
        for (Selector sel : tableMetadata.getOptions().keySet()) {
            if (sel instanceof StringSelector) {
                if (((StringSelector) sel).getValue().equals(SHARDING_ENABLED)) {
                    isSharded = true;
                }
            }
        }
        Selector sel = tableMetadata.getOptions().get(new StringSelector(SHARDING_ENABLED));
        StringSelector strSelector = new StringSelector(SHARDING_ENABLED);
        StringSelector sel2 = strSelector;
        boolean equalss = strSelector.equals(sel2);
        return isSharded;

        /*
         * TODO StringSelector.equals()? if ((selectorSharded = tableMetadata.getOptions().get(new
         * StringSelector(SHARDING_ENABLED))) != null) { return ((BooleanSelector) selectorSharded).getValue(); // TODO
         * return (Boolean) tableMetadata.getOptions().get(SHARDING_ENABLED); } else return a;
         */
    }

    private void enableSharding(ClusterName targetCluster, String catalogName) throws ExecutionException {

        DB catalog;
        try {
            catalog = recoveredClient(targetCluster).getDB("admin");
        } catch (HandlerConnectionException e) {
            throw new ExecutionException("admin cannot be recovered: " + targetCluster.getName(), e);
        }
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
    public void dropCatalog(ClusterName targetCluster, CatalogName name) throws ExecutionException {
        try {
            recoveredClient(targetCluster).dropDatabase(name.getName());
        } catch (HandlerConnectionException e) {
            throw new ExecutionException("cluster cannot be recovered: " + targetCluster.getName(), e);
        }
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
    public void dropTable(ClusterName targetCluster, TableName name) throws ExecutionException {

        DB db;
        try {
            db = recoveredClient(targetCluster).getDB(name.getCatalogName().getName());
        } catch (HandlerConnectionException e) {
            throw new ExecutionException("cluster cannot be recovered: " + targetCluster.getName(), e);
        }
        db.getCollection(name.getName()).drop();

    }

    private MongoClient recoveredClient(ClusterName targetCluster) throws HandlerConnectionException {
        return (MongoClient) connectionHandler.getConnection(targetCluster.getName()).getNativeConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IMetadataEngine#createIndex(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws ExecutionException,
                    UnsupportedException {

        DB db;
        try {
            db = recoveredClient(targetCluster)
                            .getDB(indexMetadata.getName().getTableName().getCatalogName().getName());
        } catch (HandlerConnectionException e) {
            throw new ExecutionException("cluster cannot be recovered: " + targetCluster.getName(), e);
        }

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
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws ExecutionException,
                    UnsupportedException {
        DB db;

        try {
            db = recoveredClient(targetCluster)
                            .getDB(indexMetadata.getName().getTableName().getCatalogName().getName());
        } catch (HandlerConnectionException e) {
            throw new ExecutionException("cluster cannot be recovered: " + targetCluster.getName(), e);
        }

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
