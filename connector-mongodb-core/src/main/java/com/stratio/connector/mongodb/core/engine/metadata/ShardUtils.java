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
import com.stratio.connector.mongodb.core.configuration.ShardKeyType;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.selectors.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

/**
 * @author david
 */
public class ShardUtils {

    private ShardUtils() {
    }

    /**
     * The Log.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardUtils.class);

    /**
     * @param tableMetadata
     * @return true if sharding is required
     */
    public static boolean collectionIsSharded(Map<String, Selector> options) {

        boolean isSharded = false;

        if (options != null) {
            Selector selectorSharded = options.get(SHARDING_ENABLED.getOptionName());

            if (selectorSharded != null) {
                isSharded = ((BooleanSelector) selectorSharded).getValue();
            }
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
    public static void shardCollection(MongoClient mongoClient, TableMetadata tableMetadata) throws ExecutionException,
            UnsupportedException {

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
        final DBObject cmd = new BasicDBObject("shardCollection",
                catalogName + "." + tableMetadata.getName().getName());
        cmd.put("key", shardKey);

        CommandResult result = mongoClient.getDB("admin").command(cmd);
        if (!result.ok()) {
            LOGGER.error("Command Error:" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }

    }

    public static void enableSharding(MongoClient mongoClient, String catalogName) throws ExecutionException {

        DB db;

        db = mongoClient.getDB("admin");

        CommandResult result = db.command(new BasicDBObject("enableSharding", catalogName));
        if (!result.ok() && !result.getErrorMessage().equals("already enabled")) {
            LOGGER.error("Command Error:" + result.getErrorMessage());
            throw new ExecutionException(result.getErrorMessage());
        }
    }

    /**
     * @param options
     * @return the key type. Returns a default value if not specified
     */
    public static ShardKeyType getShardKeyType(Map<String, Selector> options) {

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
            LOGGER.info("Using the asc key as the default type");
            return (ShardKeyType) SHARD_KEY_TYPE.getDefaultValue();
        }

    }

    /**
     * @param options
     * @param shardKeyType
     * @return the fields used to compute the shard key. If the option does not exist the _id is returned
     * @throws MongoValidationException
     */
    public static String[] getShardKeyFields(Map<String, Selector> options, ShardKeyType shardKeyType)
            throws MongoValidationException {

        String[] shardKey = null;

        if (options != null) {
            Selector selectorSharded = options.get(SHARD_KEY_FIELDS.getOptionName());

            if (selectorSharded != null) {
                shardKey = ((StringSelector) selectorSharded).getValue().split(",");
            }
        }

        if (shardKey == null || shardKey.length == 0) {
            LOGGER.info("Using the _id as the default shard key");
            shardKey = ((String[]) SHARD_KEY_FIELDS.getDefaultValue());
        } else if (shardKeyType == ShardKeyType.HASHED && shardKey.length > 1) {
            throw new MongoValidationException("The hashed key must have a single field");
        }
        return shardKey;

    }

}
