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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.stratio.connector.commons.metadata.IndexMetadataBuilder;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

/**
 * Utilities to discover MongoDB schemas.
 */
public final class DiscoverMetadataUtils {

    private DiscoverMetadataUtils() {

    }

    /**
     * Discover the existing fields stored in the collection.
     *
     * @param collection
     *            the collection
     * @return the list of fields including the _id
     */
    public static List<String> discoverField(DBCollection collection) {
        String map = "function() { for (var field in this) { emit(field, null); }}";
        String reduce = "function(field, stuff) { return null; }";
        MapReduceCommand mapReduceCommand = new MapReduceCommand(collection, map, reduce, null, OutputType.INLINE, null);
        DBObject getFieldsCommand = mapReduceCommand.toDBObject();
        CommandResult command = collection.getDB().command(getFieldsCommand);

        BasicDBList results = (BasicDBList) command.get("results");
        Set<String> fields = new HashSet<>();
        if (results != null) {
            for (Object object : results) {
                DBObject bson = (DBObject) object;
                fields.add((String) bson.get("_id"));
            }
        }
        return new ArrayList<String>(fields);
    }

    /**
     * Discover the existing indexes stored in the collection.
     *
     * @param collection
     *            the collection
     * @return the list of indexMetadata.
     */
    public static List<IndexMetadata> discoverIndexes(DBCollection collection) {
        // TODO add TextIndex, Geospatial,etc...
        // TODO supported only simple, compound and hashed index
        // TODO remove _id?
        // TODO return options?? e.g sparse, unique??
        // TODO custom (asc and desc)
        List<DBObject> indexInfo = collection.getIndexInfo();
        String db = collection.getDB().getName();
        String collName = collection.getName();

        List<IndexMetadata> indexMetadataList = new ArrayList<>(indexInfo.size());
        for (DBObject dbObject : indexInfo) {
            BasicDBObject key = (BasicDBObject) dbObject.get("key");

            IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(db, collName,
                            (String) dbObject.get("name"), getIndexType(key));
            for (String field : key.keySet()) {
                indexMetadataBuilder.addColumn(field, null);
            }
            indexMetadataList.add(indexMetadataBuilder.build());
        }
        return indexMetadataList;
    }

    /**
     * Gets the index type.
     *
     * @param key
     *            the key
     * @return DEFAULT when involved fields have ascending index. CUSTOM otherwise
     */
    private static IndexType getIndexType(BasicDBObject key) {
        boolean isDefault = true;
        IndexType indexType;

        Iterator<Object> iterator = key.values().iterator();

        while (iterator.hasNext() && isDefault) {
            isDefault = iterator.next().toString().startsWith("1");
        }

        if (isDefault) {
            indexType = IndexType.DEFAULT;
        } else {
            indexType = IndexType.CUSTOM;
        }
        return indexType;
    }
}
