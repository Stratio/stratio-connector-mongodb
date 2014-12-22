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

package com.stratio.connector.mongodb.core.engine.metadata;

import static com.stratio.connector.mongodb.core.configuration.ShardKeyType.ASC;
import static com.stratio.connector.mongodb.core.configuration.ShardKeyType.HASHED;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARDING_ENABLED;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARD_KEY_FIELDS;
import static com.stratio.connector.mongodb.core.configuration.TableOptions.SHARD_KEY_TYPE;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.mongodb.core.configuration.ShardKeyType;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * The Class ShardUtils.
 *
 */
public final class ShardUtils {

    private ShardUtils() {
    }

    /**
     * The Log.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardUtils.class);

    /**
     * Checks if the collection is sharded.
     *
     * @param options
     *            the options
     * @return true if the sharding is required
     * @throws ExecutionException
     */
    public static boolean isCollectionSharded(Map<String, Selector> options) throws ExecutionException {

        boolean isSharded = false;

        if (options != null) {
            Selector selectorSharded = options.get(SHARDING_ENABLED.getOptionName());

            if (selectorSharded != null) {
                isSharded = SelectorHelper.getValue(Boolean.class, selectorSharded);
            }
        }

        return isSharded;

    }

    /**
     * Shard a collection.
     *
     * @param mongoClient
     *            the mongo client
     * @param tableMetadata
     *            the table metadata
     * @throws ExecutionException
     *             if an error exist when sharding the collection
     */
    public static void shardCollection(MongoClient mongoClient, TableMetadata tableMetadata) throws ExecutionException {

        final String catalogName = tableMetadata.getName().getCatalogName().getName();
        enableSharding(mongoClient, catalogName);

        DBObject shardKey = new BasicDBObject();
        Map<String, Selector> options = SelectorOptionsUtils.processOptions(tableMetadata.getOptions());

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
            LOGGER.error("Error executing" + cmd + " :" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }

    }

    /**
     * Enable sharding in the database.
     *
     * @param mongoClient
     *            the mongo client
     * @param databaseName
     *            the database name
     * @throws ExecutionException
     *             if an error exist when enabling sharding
     */
    private static void enableSharding(MongoClient mongoClient, String databaseName) throws ExecutionException {

        DB db;

        db = mongoClient.getDB("admin");
        DBObject enableShardingCommand = new BasicDBObject("enableSharding", databaseName);
        CommandResult result = db.command(enableShardingCommand);
        if (!result.ok() && !result.getErrorMessage().equals("already enabled")) {
            LOGGER.error("Error executing" + enableShardingCommand + " :" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }
    }

    /**
     * Returns the shard key type.
     *
     * @param options
     *            the options
     * @return the shard key type chosen. Returns a default value if not specified
     */
    private static ShardKeyType getShardKeyType(Map<String, Selector> options) {

        String shardKeyType = null;
        if (options != null) {
            Selector selectorSharded = options.get(SHARD_KEY_TYPE.getOptionName());
            if (selectorSharded != null) {
                shardKeyType = ((StringSelector) selectorSharded).getValue();
            }
        }

        if (HASHED.getKeyType().equals(shardKeyType)) {
            return HASHED;
        } else if (ASC.getKeyType().equals(shardKeyType)) {
            return ASC;
        } else {
            LOGGER.warn("Using the asc key as the default type");
            return (ShardKeyType) SHARD_KEY_TYPE.getDefaultValue();
        }

    }

    /**
     * Returns the shard key fields.
     *
     * @param options
     *            the options
     * @param shardKeyType
     *            the shard key type
     * @return the fields used to compute the shard key. If not specified, the _id is returned
     * @throws MongoValidationException
     *             if the specified operation is not supported
     */
    private static String[] getShardKeyFields(Map<String, Selector> options, ShardKeyType shardKeyType)
                    throws MongoValidationException {

        String[] shardKey = null;

        if (options != null) {
            Selector selectorSharded = options.get(SHARD_KEY_FIELDS.getOptionName());
            if (selectorSharded != null) {
                shardKey = ((StringSelector) selectorSharded).getValue().split(",");
            }
        }

        if (shardKey == null || shardKey.length == 0) {
            shardKey = ((String[]) SHARD_KEY_FIELDS.getDefaultValue());
            LOGGER.warn("Using the _id as the default shard key");
        } else if (shardKeyType == ShardKeyType.HASHED && shardKey.length > 1) {
            throw new MongoValidationException("The hashed key must have a single field");
        }
        return shardKey;

    }

}
