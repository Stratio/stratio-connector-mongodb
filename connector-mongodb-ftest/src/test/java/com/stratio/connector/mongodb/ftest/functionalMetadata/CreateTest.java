/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.mongodb.ftest.functionalMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataCreateTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.core.configuration.TableOptions;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

public class CreateTest extends GenericMetadataCreateTest {

    @Override
    protected IConnectorHelper getConnectorHelper() {
        MongoConnectorHelper mongoConnectorHelper = null;
        try {
            mongoConnectorHelper = new MongoConnectorHelper(getClusterName());
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        } catch (CreateNativeConnectionException e) {
            e.printStackTrace();
        }
        return mongoConnectorHelper;
    }

    @Override
    public void createCatalogWithOptionsTest() throws UnsupportedException, ExecutionException {
        throw new RuntimeException("Not supported in MongoDB");
    }

    @Test
    public void createShardedTable() throws ConnectorException {
        ClusterName clusterName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST createCatalogWithTablesAndIndexTest ***********************************");

        TableName tableName = new TableName(CATALOG, TABLE);
        ClusterName clusterRef = getClusterName();
        List<ColumnName> partitionKey = Collections.EMPTY_LIST;
        List<ColumnName> clusterKey = Collections.EMPTY_LIST;

        // ColumnMetadata (all columns)
        Map<Selector, Selector> options;
        Map<ColumnName, ColumnMetadata> columnsMap = new HashMap<>();
        int i = 1;
        Collection<ColumnType> allSupportedColumnType = getConnectorHelper().getAllSupportedColumnType();
        for (ColumnType columnType : allSupportedColumnType) {
            ColumnName columnName = new ColumnName(CATALOG, TABLE, "columnName_" + i);
            columnsMap.put(columnName, new ColumnMetadata(columnName, null, columnType));
            i++;
        }

        options = new HashMap<Selector, Selector>();

        StringSelector optionSelector = new StringSelector(TableOptions.SHARDING_ENABLED.getOptionName());
        optionSelector.setAlias(TableOptions.SHARDING_ENABLED.getOptionName());
        options.put(optionSelector, new BooleanSelector(true));

        StringSelector optionSelector2 = new StringSelector(TableOptions.SHARD_KEY_TYPE.getOptionName());
        optionSelector2.setAlias(TableOptions.SHARD_KEY_TYPE.getOptionName());
        options.put(optionSelector2, new StringSelector("desc"));

        StringSelector optionSelector3 = new StringSelector(TableOptions.SHARD_KEY_FIELDS.getOptionName());
        optionSelector3.setAlias(TableOptions.SHARD_KEY_FIELDS.getOptionName());
        options.put(optionSelector3, new StringSelector("campo1,campo2"));

        Map<TableName, TableMetadata> tableMap = new HashMap<TableName, TableMetadata>();
        TableMetadata tableMetadata = new TableMetadata(tableName, options, columnsMap, null, clusterRef, partitionKey,
                        clusterKey);
        tableMap.put(tableName, tableMetadata);

        connector.getMetadataEngine().createTable(getClusterName(), tableMetadata);
    }
}