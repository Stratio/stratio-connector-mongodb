
package com.stratio.connector.mongodb.ftest.functionalMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericMetadataAutodiscoverFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.commons.metadata.TableMetadataBuilder;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;


public class AutoDiscoverFT extends GenericMetadataAutodiscoverFT{

	
	@Override
	protected IConnectorHelper getConnectorHelper() {
		MongoConnectorHelper mongoConnectorHelper = null;
		try {
			mongoConnectorHelper = new MongoConnectorHelper(getClusterName());
		} catch (ConnectionException e) {
			e.printStackTrace();
		} catch (InitializationException e) {
			e.printStackTrace();
		}
		return mongoConnectorHelper;
	}

	@Override
	protected TableMetadata prepareEnvironment() throws ConnectorException {
		
		TableMetadata tableMetadata = createTableMetadata();

		insertRows(tableMetadata);
		
		addIndexMetadata(tableMetadata);
		
		return tableMetadata;
	}

	private void addIndexMetadata(TableMetadata tableMetadata)
			throws ConnectorException, UnsupportedException {
		
		IndexName indexName = new IndexName(tableMetadata.getName(), INDEX_NAME);
		Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
		Object[] parameters2 = null;
		
		ColumnName columnName1 = new ColumnName(tableMetadata.getName(), COLUMN_1);
		columns.put(columnName1, new ColumnMetadata(columnName1, parameters2, ColumnType.VARCHAR));
		
		ColumnName columnName2 = new ColumnName(tableMetadata.getName(), COLUMN_2);
		columns.put(columnName2, new ColumnMetadata(columnName2, parameters2, ColumnType.TEXT));
		
		IndexMetadata indexMetadata = new IndexMetadata(indexName, columns,
				IndexType.DEFAULT, Collections.EMPTY_MAP);
		
		
		getConnector().getMetadataEngine().createIndex(getClusterName(), indexMetadata);
		
		tableMetadata.addIndex(indexName, indexMetadata);
	}

	private void insertRows(TableMetadata tableMetadata)
			throws ConnectorException, UnsupportedException {
		
		Row row1 = new Row();
		Row row2 = new Row();

        Map<String, Cell> cells_1 = new HashMap();
        Map<String, Cell> cells_2 = new HashMap();

        cells_1.put(COLUMN_1, new Cell(1));
        cells_1.put(COLUMN_2, new Cell("Paco"));
        row1.setCells(cells_1);
        
        cells_2.put(COLUMN_1, new Cell(2));
        cells_2.put(COLUMN_2, new Cell("Pepe"));
        row2.setCells(cells_2);
        
		boolean ifNotExists = true;
		getConnector().getStorageEngine().insert(getClusterName(), tableMetadata, row1, ifNotExists);
		getConnector().getStorageEngine().insert(getClusterName(), tableMetadata, row2, ifNotExists);
	}

	private TableMetadata createTableMetadata() {
		
		TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(sCatalogName,
				sTableName, sClusterName);
		
		tableMetadataBuilder.addColumn(COLUMN_1, ColumnType.INT).addColumn(COLUMN_2, ColumnType.TEXT);
		tableMetadataBuilder.withPartitionKey(COLUMN_1);
		TableMetadata tableMetadata = tableMetadataBuilder.build();
		
		return tableMetadata;
	}

}

