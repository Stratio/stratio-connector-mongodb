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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.configuration.CustomMongoIndexType;
import com.stratio.connector.mongodb.core.configuration.IndexOptions;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.connector.mongodb.testutils.IndexMetadataBuilder;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

/**
 * @author david
 */
public class IndexUtilsTest {

    private static final String DB_NAME = "catalog_name";
    private static final String TABLE_NAME = "tablename";
    private final String INDEX_NAME = "indexname";
    private final IndexType INDEX_DEFAULT = IndexType.DEFAULT;
    private final ColumnType COLUMN_VARCHAR = ColumnType.VARCHAR;
    private final String COLUMN_NAME = "colname";
    private final String COLUMN_NAME2 = "colname2";

    @Test
    public void getBasicCustomOptionsTest() throws MongoValidationException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_DEFAULT);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getCustomOptions(indexMetadata);

        assertEquals("The index name is not the expected", object.get("name"), INDEX_NAME);

    }

    @Test
    public void getCustomOptionsTest() throws MongoValidationException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_DEFAULT);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        indexMetaBuilder.addOption(IndexOptions.SPARSE.getOptionName(), true).addOption(
                        IndexOptions.UNIQUE.getOptionName(), false);
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getCustomOptions(indexMetadata);

        assertEquals("The index name is not the expected", object.get("name"), INDEX_NAME);
        assertEquals("The 'sparse' property should be true", object.get("sparse"), true);
        assertEquals("The 'unique' property should be false", object.get("unique"), false);

    }

    @Test
    public void getIndexDBObjectDefaultTest() throws UnsupportedException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME, INDEX_DEFAULT);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        indexMetaBuilder.addColumn(COLUMN_NAME2, COLUMN_VARCHAR);
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getIndexDBObject(indexMetadata);

        assertEquals("The value of the default index column should be 1", 1, object.get(COLUMN_NAME));
        assertEquals("The value of the default index column should be 1", 1, object.get(COLUMN_NAME2));

    }

    @Test
    public void getIndexDBObjectFullTextTest() throws UnsupportedException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.FULL_TEXT);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getIndexDBObject(indexMetadata);

        assertEquals("The value of the full-text index column should be 'text'", "text", object.get(COLUMN_NAME));

    }

    @Test
    public void getHashedIndexDBObjectTest() throws UnsupportedException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.CUSTOM);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        indexMetaBuilder.addOption(IndexOptions.INDEX_TYPE.getOptionName(), CustomMongoIndexType.HASHED.getIndexType());
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getIndexDBObject(indexMetadata);
        assertEquals("The value of the hashed index column should be 'hashed'", "hashed", object.get(COLUMN_NAME));
    }

    @Test
    public void getCustomDescendIndexDBObjectTest() throws UnsupportedException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.CUSTOM);
        indexMetaBuilder.addColumn(COLUMN_NAME, COLUMN_VARCHAR);
        indexMetaBuilder.addOption(IndexOptions.INDEX_TYPE.getOptionName(),
                        CustomMongoIndexType.COMPOUND.getIndexType());
        indexMetaBuilder.addOption(IndexOptions.COMPOUND_FIELDS.getOptionName(), COLUMN_NAME + ":desc");
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getIndexDBObject(indexMetadata);

        assertEquals("The value of the descending index column should be '-1'", -1, object.get(COLUMN_NAME));

    }

    @Test
    public void getCustomCompoundIndexDBObjectTest() throws UnsupportedException {
        IndexMetadataBuilder indexMetaBuilder = new IndexMetadataBuilder(DB_NAME, TABLE_NAME, INDEX_NAME,
                        IndexType.CUSTOM);
        indexMetaBuilder.addOption(IndexOptions.INDEX_TYPE.getOptionName(),
                        CustomMongoIndexType.COMPOUND.getIndexType());
        indexMetaBuilder.addOption(IndexOptions.COMPOUND_FIELDS.getOptionName(), COLUMN_NAME + ":desc," + COLUMN_NAME2
                        + ":asc");
        IndexMetadata indexMetadata = indexMetaBuilder.build();

        DBObject object = IndexUtils.getIndexDBObject(indexMetadata);

        assertEquals("The value of the descending index " + COLUMN_NAME + " should be '-1'", -1,
                        object.get(COLUMN_NAME));
        assertEquals("The value of the ascending index " + COLUMN_NAME2 + " should be '1'", 1, object.get(COLUMN_NAME2));

    }
}
