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

import java.util.*;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.util.Hash;
import com.stratio.connector.commons.metadata.IndexMetadataBuilder;
import com.stratio.connector.mongodb.core.configuration.ConfigurationOptions;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.SAMPLE_PROBABILITY;

/**
 * Utilities to discover MongoDB schemas.
 */
public final class DiscoverMetadataUtils {

    /**
     * Constructor.
     */
    private DiscoverMetadataUtils() {

    }



    /**
     * Discover the existing fields stored in the collection and their data types.
     *
     * @param collection  the collection
     * @param sample_probability the sample per cent.
     * @return the list of fields including the _id
     */
    public static Map<String, String> discoverFieldsWithType(DBCollection collection, String sample_probability) {
        String map = "function() {  if(Math.random() <= sample_number) {for (var key in this) {var type = typeof(this[key]); if(type == \"object\"){type = \"string\";};emit(key, type);}} } ";
        String reduce = "function(key, values) { var result = \"\"; for (var i = 0; i < values.length; i++){ var v = values[i];if(v == \"string\"){result = \"string\"; break;} if(v == \"number\"){result = \"number\"} if(v == \"boolean\" && result == \"number\"){result = \"string\"; break;}if(v == \"number\" && result == \"boolean\"){result = \"string\"; break;} if(v==\"boolean\"){result = \"boolean\"}};return result; }";
        MapReduceCommand mapReduceCommand = new MapReduceCommand(collection, map, reduce, null, OutputType.INLINE, null);
        HashMap<String, Object> scope = new HashMap<>();
//        connection
        scope.put("sample_number", sample_probability);
        mapReduceCommand.setScope(scope);

        DBObject getFieldsCommand = mapReduceCommand.toDBObject();
        CommandResult command = collection.getDB().command(getFieldsCommand);

        BasicDBList results = (BasicDBList) command.get("results");
        HashMap<String, String> fields = new HashMap<>();
        if (results != null) {
            for (Object object : results) {
                DBObject bson = (DBObject) object;
                String nameField = (String) bson.get("_id");
                String type = (String) bson.get("value");
                fields.put(nameField, type);
            }
        }
        return fields;
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
     * @param key   the key
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

    /**
     * Recover the sample property.
     * @param connectorClusterConfig the connector config send from crossdata.
     * @return the sample property if exists, else the default value.
     */
    public static String recoveredSampleProperty(ConnectorClusterConfig connectorClusterConfig) {
        String sampleProperty;
        if(connectorClusterConfig.getConnectorOptions() == null ||  !connectorClusterConfig.getConnectorOptions().containsKey(SAMPLE_PROBABILITY.getOptionName()) || connectorClusterConfig.getConnectorOptions().get(SAMPLE_PROBABILITY.getOptionName()) == null) {
            sampleProperty = SAMPLE_PROBABILITY.getDefaultValue()[0];
        }else{
            sampleProperty = connectorClusterConfig.getConnectorOptions().get(SAMPLE_PROBABILITY.getOptionName());
        }
        return sampleProperty;
    }
}
