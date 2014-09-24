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
import com.stratio.connector.mongodb.core.engine.MongoMetadataEngine;
import com.stratio.connector.mongodb.ftest.MongoConnectorHelper;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

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
    public void createShardedTable() throws UnsupportedException, ExecutionException {
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

        options = new HashMap<>();
        options.put(new StringSelector(MongoMetadataEngine.SHARDING_ENABLED), new BooleanSelector(true));

        Map<TableName, TableMetadata> tableMap = new HashMap<TableName, TableMetadata>();
        TableMetadata tableMetadata = new TableMetadata(tableName, options, columnsMap, null, clusterRef, partitionKey,
                        clusterKey);
        tableMap.put(tableName, tableMetadata);

        connector.getMetadataEngine().createCatalog(getClusterName(),
                        new CatalogMetadata(new CatalogName(CATALOG), Collections.EMPTY_MAP, tableMap));
    }
}