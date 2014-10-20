/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.mongodb.testutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

/**
 * @author darroyo
 */
public class TableMetadataBuilder {
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TableName tableName;
    private Map<Selector, Selector> options = Collections.EMPTY_MAP;
    private Map<ColumnName, ColumnMetadata> columns = Collections.EMPTY_MAP;
    private Map<IndexName, IndexMetadata> indexes = Collections.EMPTY_MAP;
    private List<ColumnName> partitionKey = Collections.EMPTY_LIST;
    private List<ColumnName> clusterKey = Collections.EMPTY_LIST;
    private ClusterName clusterName = null;

    public TableMetadataBuilder(String catalogName, String tableName) {
        this.tableName = new TableName(catalogName, tableName);
        columns = new HashMap<ColumnName, ColumnMetadata>();
        indexes = new HashMap<IndexName, IndexMetadata>();
        partitionKey = new ArrayList<ColumnName>();
        clusterKey = new ArrayList<ColumnName>();
        options = null;
    }

    public TableMetadataBuilder withOptions(Map<Selector, Selector> opts) {
        options = new HashMap<Selector, Selector>(opts);
        return this;
    }

    public TableMetadataBuilder withColumns(List<ColumnMetadata> columnsMetadata) {
        for (ColumnMetadata colMetadata : columnsMetadata) {
            columns.put(colMetadata.getName(), colMetadata);
        }
        return this;
    }

    /**
     * parameters in columnMetadata will be null
     *
     * @param columnName
     * @param colType
     * @return
     */
    public TableMetadataBuilder addColumn(String columnName, ColumnType colType) {
        ColumnName colName = new ColumnName(tableName, columnName);
        ColumnMetadata colMetadata = new ColumnMetadata(colName, null, colType);
        columns.put(colName, colMetadata);
        return this;
    }

    /**
     * Must be called after including columns options in indexMetadata will be null TODO same as options?.
     * ColumnMetadata is recovered from the tableMetadata
     *
     * @param type
     * @param indexName
     * @param fields
     *            the columns which define the index
     * @return
     */
    public TableMetadataBuilder addIndex(IndexType indType, String indexName, String... fields) {

        IndexName indName = new IndexName(tableName.getName(), tableName.getName(), indexName);

        Map<ColumnName, ColumnMetadata> columnsMetadata = new HashMap<ColumnName, ColumnMetadata>(fields.length);
        // recover the columns from the table metadata
        for (String field : fields) {
            ColumnMetadata cMetadata = columns.get(new ColumnName(tableName, field));
            if (cMetadata == null) {
                throw new RuntimeException("Trying to index a not existing column: " + field);
            }
            columnsMetadata.put(new ColumnName(tableName, field), cMetadata);
        }
        IndexMetadata indMetadata = new IndexMetadata(indName, columnsMetadata, indType, null);
        indexes.put(indName, indMetadata);
        return this;
    }

    // TODO implement the generic
    /*
     * public TableMetadataBuilder withIndexes(){ }
     */

    public TableMetadataBuilder withPartitionKey(String... fields) {
        for (String field : fields) {
            partitionKey.add(new ColumnName(tableName, field));
        }
        return this;
    }

    public TableMetadataBuilder withClusterKey(String... fields) {
        for (String field : fields) {
            clusterKey.add(new ColumnName(tableName, field));
        }
        return this;
    }

    public TableMetadataBuilder withClusterNameRef(ClusterName clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public TableMetadata build() {
        // TODO logger.debug()
        return new TableMetadata(tableName, options, columns, indexes, clusterName, partitionKey, clusterKey);

    }

}
