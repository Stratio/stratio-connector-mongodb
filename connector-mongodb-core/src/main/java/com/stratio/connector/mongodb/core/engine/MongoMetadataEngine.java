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

import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.COMPOUND;
import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.GEOSPATIAL_FLAT;
import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.GEOSPATIAL_SPHERE;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.COMPOUND_FIELDS;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.INDEX_TYPE;
import static com.stratio.connector.mongodb.core.configuration.ShardKeyType.ASC;
import static com.stratio.connector.mongodb.core.configuration.ShardKeyType.HASHED;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARDING_ENABLED;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARD_KEY_FIELDS;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARD_KEY_TYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType;
import com.stratio.connector.mongodb.core.configuration.ShardKeyType;
import com.stratio.connector.mongodb.core.connection.MongoConnectionHandler;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
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
public class MongoMetadataEngine extends CommonsMetadataEngine<MongoClient> {

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
     * Create a database in MongoDB.
     *
     * @param catalogMetadata
     *            the catalogMetadata.
     * @param connection
     *            the connection which contains the native connector.
     * @throws UnsupportedException
     *             if any operation is not supported.
     * @throws ExecutionException
     *             if an error occur.
     */
    @Override
    protected void createCatalog(CatalogMetadata catalogMetadata, Connection<MongoClient> connection)
                    throws ExecutionException, UnsupportedException {
        throw new UnsupportedException("Create catalog is not supported");
    }

    /**
     * Create a collection in MongoDB.
     *
     * @param tableMetadata
     *            the tableMetadata.
     * @param connection
     *            the connection which contains the native connector.
     * @throws UnsupportedException
     *             if any operation is not supported.
     * @throws ExecutionException
     *             if an error occur.
     */
    @Override
    protected void createTable(TableMetadata tableMetadata, Connection<MongoClient> connection)
                    throws ExecutionException, UnsupportedException {

        if (tableMetadata == null)
            throw new UnsupportedException("the table metadata is required");

        if (tableMetadata.getName() == null)
            throw new UnsupportedException("the table name is required");

        if (collectionIsSharded(processOptions(tableMetadata.getOptions())))
            shardCollection((MongoClient) connection.getNativeConnection(), tableMetadata);
    }

    /**
     * @param options
     * @return
     */
    private Map<String, Selector> processOptions(Map<Selector, Selector> options) {
        Map<String, Selector> stringOptions = new HashMap<String, Selector>();

        for (Entry<Selector, Selector> e : options.entrySet()) {
            stringOptions.put(e.getKey().getAlias(), e.getValue());
        }
        return stringOptions;
    }

    /**
     * @param tableMetadata
     * @return true if sharding is required
     */
    private boolean collectionIsSharded(Map<String, Selector> options) {

        boolean isSharded = false;

        if (options != null) {
            Selector selectorSharded = null;

            if ((selectorSharded = options.get(SHARDING_ENABLED.getOptionName())) != null)
                isSharded = ((BooleanSelector) selectorSharded).getValue();
        }

        return isSharded;

    }

    /**
     * Shard a collection
     * 
     * @param mongoClient
     * @param tableMetadata
     * @throws ExecutionException
     * @throws UnsupportedException
     */
    private void shardCollection(MongoClient mongoClient, TableMetadata tableMetadata) throws ExecutionException,
                    UnsupportedException {

        final String catalogName = tableMetadata.getName().getCatalogName().getName();
        enableSharding(mongoClient, catalogName);

        DBObject shardKey = new BasicDBObject();

        Map<String, Selector> options = processOptions(tableMetadata.getOptions());

        ShardKeyType shardKeyType = getShardKeyType(options);

        String[] shardKeyFields = getShardKeyFields(options, shardKeyType);

        switch (shardKeyType) {
        case ASC:
            for (String field : shardKeyFields) {
                shardKey.put(field, 1);
            }
            break;
        case HASHED:
            shardKey.put(shardKeyFields[0], "hashed");
            break;
        }

        // shard the collection with the key
        final DBObject cmd = new BasicDBObject("shardCollection", catalogName + "." + tableMetadata.getName().getName());
        cmd.put("key", shardKey);

        CommandResult result = mongoClient.getDB("admin").command(cmd);
        if (!result.ok()) {
            logger.error("Command Error:" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }

    }

    private void enableSharding(MongoClient mongoClient, String catalogName) throws ExecutionException {

        DB db;

        db = mongoClient.getDB("admin");

        CommandResult result = db.command(new BasicDBObject("enableSharding", catalogName));
        if (!result.ok() && !result.getErrorMessage().equals("already enabled")) {
            logger.error("Command Error:" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }
    }

    /**
     * @param options
     * @return the key type. Returns a default value if not specified
     */
    private ShardKeyType getShardKeyType(Map<String, Selector> options) {

        String shardKeyType = null;

        if (options != null) {
            Selector selectorSharded = null;

            if ((selectorSharded = options.get(SHARD_KEY_TYPE.getOptionName())) != null)
                shardKeyType = ((StringSelector) selectorSharded).getValue();
        }

        if (HASHED.getKeyType().equals(shardKeyType))
            return HASHED;
        else if (ASC.getKeyType().equals(shardKeyType))
            return ASC;
        else {
            logger.info("Using the asc key as the default type");
            return (ShardKeyType) SHARD_KEY_TYPE.getDefaultValue();
        }

    }

    /**
     * @param options
     * @param shardKeyType
     * @return the fields used to compute the shard key. If the option does not exist the _id is returned
     * @throws MongoValidationException
     */
    private String[] getShardKeyFields(Map<String, Selector> options, ShardKeyType shardKeyType)
                    throws MongoValidationException {

        String[] shardKey = null;

        if (options != null) {
            Selector selectorSharded = null;

            if ((selectorSharded = options.get(SHARD_KEY_FIELDS.getOptionName())) != null)
                shardKey = ((StringSelector) selectorSharded).getValue().split(",");
        }

        if (shardKey == null || shardKey.length == 0) {
            logger.info("Using the _id as the default shard key");
            shardKey = ((String[]) SHARD_KEY_FIELDS.getDefaultValue());
        } else if (shardKeyType == ShardKeyType.HASHED) {
            if (shardKey.length > 1)
                throw new MongoValidationException("The hashed key must have a single field");
        }
        return shardKey;

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
    protected void dropCatalog(CatalogName name, Connection<MongoClient> connection) throws ExecutionException {
        try {
            connection.getNativeConnection().dropDatabase(name.getName());
        } catch (MongoException e) {
            throw new ExecutionException(e.getMessage(), e);
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
    protected void dropTable(TableName name, Connection<MongoClient> connection) throws ExecutionException {

        DB db = connection.getNativeConnection().getDB(name.getCatalogName().getName());
        try {
            db.getCollection(name.getName()).drop();
        } catch (MongoException e) {
            throw new ExecutionException(e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IMetadataEngine#createIndex(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection<MongoClient> connection)
                    throws ExecutionException, UnsupportedException {

        IndexType indexType = indexMetadata.getType();
        DB db = connection.getNativeConnection().getDB(
                        indexMetadata.getName().getTableName().getCatalogName().getName());

        DBObject indexDBObject = new BasicDBObject();
        DBObject indexOptionsDBObject = null;
        String indexName = indexMetadata.getName().getName();

        if (indexType == IndexType.DEFAULT) {

            for (ColumnMetadata columnMeta : indexMetadata.getColumns().values()) {
                indexDBObject.put(columnMeta.getName().getName(), 1);
            }

        } else if (indexType == IndexType.FULL_TEXT) {
            for (ColumnMetadata columnMeta : indexMetadata.getColumns().values()) {
                indexDBObject.put(columnMeta.getName().getName(), "text");
            }

        } else if (indexMetadata.getType() == IndexType.CUSTOM) {
            indexDBObject = getCustomIndexDBObject(indexMetadata);
        } else
            throw new UnsupportedException("index type " + indexMetadata.getType().toString() + " is not supported");

        if (indexName != null && !indexName.trim().isEmpty()) {
            indexOptionsDBObject = new BasicDBObject("name", indexName);
            try {
                db.getCollection(indexMetadata.getName().getTableName().getName()).createIndex(indexDBObject,
                                indexOptionsDBObject);
            } catch (MongoException e) {
                throw new ExecutionException(e.getMessage(), e);
            }
        } else
            try {
                db.getCollection(indexMetadata.getName().getTableName().getName()).createIndex(indexDBObject);
            } catch (MongoException e) {
                throw new ExecutionException(e.getMessage(), e);
            }

        logger.debug("Index created " + indexDBObject.toString());
    }

    /**
     * @return
     * @throws UnsupportedException
     */
    private DBObject getCustomIndexDBObject(IndexMetadata indexMetadata) throws UnsupportedException {
        DBObject indexDBObject = new BasicDBObject();

        Map<String, Selector> options = processOptions(indexMetadata.getOptions());

        if (options == null)
            throw new UnsupportedException("The custom index must have an index type and fields");

        Selector selectorSharded = null;
        String indexType;
        String[] fields;

        // Retrieves the type
        if ((selectorSharded = options.get(INDEX_TYPE.getOptionName())) != null) {
            indexType = ((StringSelector) selectorSharded).getValue().trim();
        } else
            throw new UnsupportedException("The custom index must have an index type");

        // Retrieves the fields
        if (COMPOUND.getIndexType().equals(indexType)) {
            if ((selectorSharded = options.get(COMPOUND_FIELDS.getOptionName())) != null) {
                fields = ((StringSelector) selectorSharded).getValue().split(",");
            } else
                throw new UnsupportedException("The custom index must have 1 o more fields");
        } else {
            int i = 0;
            fields = new String[indexMetadata.getColumns().size()];
            for (ColumnMetadata colMetadata : indexMetadata.getColumns().values()) {
                fields[i++] = colMetadata.getName().getName();
            }
        }

        // Create the index specified
        if (CustomMongoIndexType.HASHED.getIndexType().equals(indexType)) {
            if (fields.length != 1)
                throw new UnsupportedException("The hashed index must have a single field");
            indexDBObject.put(fields[0], "hashed");
        } else if (COMPOUND.getIndexType().equals(indexType)) {
            for (String field : fields) {
                String[] fieldInfo = field.split(":");
                if (fieldInfo.length != 2)
                    throw new UnsupportedException(
                                    "Format error. The fields in a compound index must be: fieldname:asc|desc [, field2:desc ...] ");
                int order = fieldInfo[1].trim().equals("asc") ? 1 : -1;
                indexDBObject.put(fieldInfo[0], order);
            }
        } else if (GEOSPATIAL_SPHERE.getIndexType().equals(indexType)) {
            if (fields.length != 1)
                throw new UnsupportedException("The geospatial index must have a single field");
            indexDBObject.put(fields[0], "2dsphere");
        } else if (GEOSPATIAL_FLAT.getIndexType().equals(indexType)) {
            if (fields.length != 1)
                throw new UnsupportedException("The geospatial index must have a single field");
            indexDBObject.put(fields[0], "2d");
        } else
            throw new UnsupportedException("Index " + indexType + " is not supported");

        return indexDBObject;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.meta.common.connector.IMetadataEngine#dropIndex(com.stratio.meta2.common.data.ClusterName,
     * com.stratio.meta2.common.metadata.IndexMetadata)
     */
    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection<MongoClient> connection)
                    throws ExecutionException, UnsupportedException {
        DB db = connection.getNativeConnection().getDB(
                        indexMetadata.getName().getTableName().getCatalogName().getName());

        String indexName = null;
        if (indexMetadata.getName() != null)
            indexName = indexMetadata.getName().getName();

        if (indexName != null) {
            try {
                db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(indexName);
            } catch (Exception e) {
                throw new ExecutionException(e.getMessage(), e);
            }
        } else {

            if (indexMetadata.getType() == IndexType.DEFAULT) {
                DBObject indexDBObject = new BasicDBObject();
                for (ColumnMetadata columnMeta : indexMetadata.getColumns().values()) {
                    indexDBObject.put(columnMeta.getName().getName(), 1);
                }
                try {
                    db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(indexDBObject);
                } catch (Exception e) {
                    throw new ExecutionException(e.getMessage(), e);
                }
            } else if (indexMetadata.getType() == IndexType.FULL_TEXT) {

                String defaultTextIndexName;
                StringBuffer strBuf = new StringBuffer();

                int colNumber = 0;
                int columnSize = indexMetadata.getColumns().size();

                for (ColumnMetadata columnMeta : indexMetadata.getColumns().values()) {
                    strBuf.append(columnMeta.getName().getName() + "_");
                    if (++colNumber != columnSize)
                        strBuf.append("_");
                }
                defaultTextIndexName = strBuf.toString();
                try {
                    db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(defaultTextIndexName);
                } catch (Exception e) {
                    throw new ExecutionException(e.getMessage(), e);
                }
            } else
                throw new UnsupportedException("Dropping without the index name is not supported for the index type: "
                                + indexMetadata.getType().toString());

        }

    }

}
