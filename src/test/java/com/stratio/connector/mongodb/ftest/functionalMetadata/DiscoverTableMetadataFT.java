package com.stratio.connector.mongodb.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericDiscoverTableMetadataFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.ftest.helper.DefaultConfigurationMongoConnectorHelper;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class DiscoverTableMetadataFT extends GenericDiscoverTableMetadataFT {

    @Override
    protected IConnectorHelper getConnectorHelper() {
        return DefaultConfigurationMongoConnectorHelper.getInstance();
    }

    @Override
    protected void prepareEnvironment(TableMetadata tableMetadata) throws ConnectorException {
        insertRows(tableMetadata);
        createIndex(tableMetadata);

    }

    @Test
    @Override
    public void provideIndexMetadataFT() throws UnsupportedException, ConnectorException {

        assertTrue("The index has not been created", iConnectorHelper.containsIndex(CATALOG, TABLE, INDEX_NAME));
        Map<IndexName, IndexMetadata> indexes = tableMetadataProvided.getIndexes();

        assertTrue("The index has not been recovered", containsIndex(indexes));

        IndexType typeProvided = resolveIndexType(indexes);
        IndexType typeExpected = resolveIndexType(tableMetadata.getIndexes());
        assertEquals("The type is not the expected", typeExpected, typeProvided);

        Set<IndexName> keySet = tableMetadataProvided.getIndexes().keySet();

        Iterator<IndexName> indexIterator = keySet.iterator();
        IndexMetadata indexMetadata = indexes.get(indexIterator.next());

        assertEquals("The index should have the _id column", 1, indexMetadata.getColumns().keySet().size());

        indexMetadata = indexes.get(indexIterator.next());
        assertEquals("The index should have 2 columns", 2, indexMetadata.getColumns().keySet().size());
        Iterator<ColumnName> iterator = indexMetadata.getColumns().keySet().iterator();

        assertEquals("The first index column should be " + COLUMN_1, COLUMN_1, iterator.next().getName());
        assertEquals("The second index column should be " + COLUMN_2, COLUMN_2, iterator.next().getName());
    }

    @Test
    @Override
    public void providePrimaryKeyFT() throws UnsupportedException, ConnectorException {

        assertEquals("The primary key should be _id", "_id", tableMetadataProvided.getPrimaryKey().get(0).getName());
    }

    @Test
    @Override
    public void provideFieldsFT() throws UnsupportedException, ConnectorException {

        assertEquals("The table must have 3 columns", 3, tableMetadataProvided.getColumns().size());

        assertTrue("The column _id has not been found",
                        tableMetadataProvided.getColumns().containsKey(new ColumnName(tableMetadata.getName(), "_id")));

        assertTrue("The column " + COLUMN_1 + " has not been found",
                        tableMetadataProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadata.getName(), COLUMN_1)));
        assertTrue("The column " + COLUMN_2 + " has not been found",
                        tableMetadataProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadata.getName(), COLUMN_2)));

    }

    private void createIndex(TableMetadata tableMetadata) throws UnsupportedException, ConnectorException {
        for (IndexMetadata indexMetadata : tableMetadata.getIndexes().values()) {
            if (indexMetadata.getType() == IndexType.FULL_TEXT) {
                throw new RuntimeException("Full text not supported");
            }
            getConnector().getMetadataEngine().createIndex(getClusterName(), indexMetadata);
        }

    }

    private void insertRows(TableMetadata tableMetadata) throws ConnectorException, UnsupportedException {
        Row row1 = new Row();
        for (ColumnName colName : tableMetadata.getColumns().keySet()) {
            row1.addCell(colName.getName(), new Cell(1));
        }

        getConnector().getStorageEngine().insert(getClusterName(), tableMetadata, row1, true);

    }

}
