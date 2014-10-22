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

import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.COMPOUND;
import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.GEOSPATIAL_FLAT;
import static com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType.GEOSPATIAL_SPHERE;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.COMPOUND_FIELDS;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.INDEX_TYPE;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.SPARSE;
import static com.stratio.connector.mongodb.core.configuration.IndexOptions.UNIQUE;

import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.statements.structures.selectors.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

/**
 * @author david
 */
public class IndexUtils {

    private IndexUtils() {
    }

    /**
     * @param indexMetadata
     * @return
     * @throws MongoValidationException
     */
    public static DBObject getCustomOptions(IndexMetadata indexMetadata) throws MongoValidationException {
        DBObject indexOptionsDBObject = new BasicDBObject();
        String indexName = indexMetadata.getName().getName();
        Map<String, Selector> options = SelectorOptionsUtils.processOptions(indexMetadata.getOptions());

        if (options != null) {
            Selector boolSelector = options.get(SPARSE.getOptionName());
            if (boolSelector != null) {
                indexOptionsDBObject.put("sparse", ((BooleanSelector) boolSelector).getValue());
            }
            boolSelector = options.get(UNIQUE.getOptionName());
            if (boolSelector != null) {
                indexOptionsDBObject.put("unique", ((BooleanSelector) boolSelector).getValue());
            }
        }

        if (indexName != null && !indexName.trim().isEmpty()) {
            indexOptionsDBObject.put("name", indexName);
        }

        return indexOptionsDBObject;

    }

    /**
     * @param indexMetadata
     * @return
     * @throws UnsupportedException
     */
    public static DBObject getIndexDBObject(IndexMetadata indexMetadata) throws UnsupportedException {
        IndexType indexType = indexMetadata.getType();

        DBObject indexDBObject = new BasicDBObject();
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
        } else {
            throw new UnsupportedException("index type " + indexMetadata.getType().toString() + " is not supported");
        }

        return indexDBObject;
    }

    /**
     * @return
     * @throws UnsupportedException
     */
    private static DBObject getCustomIndexDBObject(IndexMetadata indexMetadata) throws UnsupportedException {
        DBObject indexDBObject = new BasicDBObject();
        Map<String, Selector> options = SelectorOptionsUtils.processOptions(indexMetadata.getOptions());

        if (options == null) {
            throw new UnsupportedException("The custom index must have an index type and fields");
        }
        Selector selector = options.get(INDEX_TYPE.getOptionName());
        if (selector == null) {
            throw new UnsupportedException("The custom index must have an index type");
        }

        String indexType = ((StringSelector) selector).getValue().trim();
        String[] fields = getCustomIndexDBObjectFields(options, indexType, selector, indexMetadata);

        // Create the index specified
        if (COMPOUND.getIndexType().equals(indexType)) {
            for (String field : fields) {
                String[] fieldInfo = field.split(":");
                if (fieldInfo.length != 2) {
                    throw new UnsupportedException(
                            "Format error. The fields in a compound index must be: fieldname:asc|desc [, field2:desc ...] ");
                }
                int order = fieldInfo[1].trim().equals("asc") ? 1 : -1;
                indexDBObject.put(fieldInfo[0], order);
            }
        } else {
            if (fields.length != 1) {
                throw new UnsupportedException("The " + indexType + " index must have a single field");
            }
            String mongoIndexType;
            if (CustomMongoIndexType.HASHED.getIndexType().equals(indexType)) {
                mongoIndexType = "hashed";
            } else if (GEOSPATIAL_SPHERE.getIndexType().equals(indexType)) {
                mongoIndexType = "2dsphere";
            } else if (GEOSPATIAL_FLAT.getIndexType().equals(indexType)) {
                mongoIndexType = "2d";
            } else {
                throw new UnsupportedException("Index " + indexType + " is not supported");
            }
            indexDBObject.put(fields[0], mongoIndexType);
        }

        return indexDBObject;
    }

    /**
     * @param options
     * @param selectorSharded
     * @return
     * @throws UnsupportedException
     */
    private static String[] getCustomIndexDBObjectFields(Map<String, Selector> options, String indexType,
            Selector selector, IndexMetadata indexMetadata) throws UnsupportedException {
        String[] fields;
        if (COMPOUND.getIndexType().equals(indexType)) {
            if ((selector = options.get(COMPOUND_FIELDS.getOptionName())) != null) {
                fields = ((StringSelector) selector).getValue().split(",");
            } else {
                throw new UnsupportedException("The custom index must have 1 o more fields");
            }
        } else {
            int i = 0;
            fields = new String[indexMetadata.getColumns().size()];
            for (ColumnMetadata colMetadata : indexMetadata.getColumns().values()) {
                fields[i++] = colMetadata.getName().getName();
            }
        }
        return fields;
    }

    /**
     * @param indexMetadata
     * @param db
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    public static void dropIndexWithDefaultName(IndexMetadata indexMetadata, DB db) throws UnsupportedException,
            ExecutionException {
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
                strBuf.append(columnMeta.getName().getName()).append("_");
                if (++colNumber != columnSize) {
                    strBuf.append("_");
                }
            }
            defaultTextIndexName = strBuf.toString();
            try {
                db.getCollection(indexMetadata.getName().getTableName().getName()).dropIndex(defaultTextIndexName);
            } catch (Exception e) {
                throw new ExecutionException(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedException("Dropping without the index name is not supported for the index type: "
                    + indexMetadata.getType().toString());
        }

    }

}
