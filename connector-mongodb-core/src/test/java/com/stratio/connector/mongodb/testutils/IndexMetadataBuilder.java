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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.statements.structures.selectors.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.selectors.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

/**
 * @author darroyo
 */
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

/**
 * @author darroyo
 */
public class IndexMetadataBuilder {
    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private IndexName indexName;
    private TableName tableName;
    private Map<Selector, Selector> options = Collections.EMPTY_MAP;
    private Map<ColumnName, ColumnMetadata> columns = Collections.EMPTY_MAP;
    private IndexType indexType;

    // TODO create the catalog and the table if needed

    public IndexMetadataBuilder(String catalogName, String tableName, String indexName, IndexType type) {
        this.tableName = new TableName(catalogName, tableName);
        this.indexName = new IndexName(this.tableName, indexName);
        columns = new HashMap<ColumnName, ColumnMetadata>();
        options = null;
        this.indexType = type;
    }

    public IndexMetadataBuilder withOptions(Map<Selector, Selector> opts) {
        options = new HashMap<Selector, Selector>(opts);
        return this;
    }

    public IndexMetadataBuilder withColumns(List<ColumnMetadata> columnsMetadata) {
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
    public IndexMetadataBuilder addColumn(String columnName, ColumnType colType) {
        ColumnName colName = new ColumnName(tableName, columnName);
        ColumnMetadata colMetadata = new ColumnMetadata(colName, null, colType);
        columns.put(colName, colMetadata);
        return this;
    }

    public IndexMetadataBuilder addOption(String option, String value) {
        if (options == null)
            options = new HashMap<Selector, Selector>();
        options.put(new StringSelector(option), new StringSelector(value));
        return this;
    }

    public IndexMetadataBuilder addOption(String option, Integer value) {
        if (options == null)
            options = new HashMap<Selector, Selector>();
        options.put(new StringSelector(option), new IntegerSelector(value));
        return this;
    }

    public IndexMetadataBuilder addOption(String option, Boolean value) {
        if (options == null)
            options = new HashMap<Selector, Selector>();
        options.put(new StringSelector(option), new BooleanSelector(value));
        return this;
    }

    public IndexMetadata build() {
        // TODO logger.debug()
        return new IndexMetadata(indexName, columns, indexType, options);

    }

}
