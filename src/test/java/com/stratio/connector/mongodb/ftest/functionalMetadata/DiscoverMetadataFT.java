package com.stratio.connector.mongodb.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.commons.ftest.functionalMetadata.GenericDiscoverMetadataFT;
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

public final class DiscoverMetadataFT extends GenericDiscoverMetadataFT {

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

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        verifyIndexMetadata(tableWithIndexProvided);

        Set<IndexName> keySet = tableSimpleProvided.getIndexes().keySet();
        assertEquals("The index should have the _id column", 1, keySet.size());

        IndexMetadata indexMetadata = tableSimpleProvided.getIndexes().get(keySet.iterator().next());
        assertTrue("The index should have the _id column",
                        indexMetadata.getColumns().keySet().contains(new ColumnName(CATALOG, SECOND_TABLE, "_id")));

    }

    private void verifyIndexMetadata(TableMetadata tableMetadataProvided) {

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

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        assertEquals("The primary key should be _id", "_id", tableWithIndexProvided.getPrimaryKey().get(0).getName());
        assertEquals("The primary key should be _id", "_id", tableSimpleProvided.getPrimaryKey().get(0).getName());
    }

    @Test
    @Override
    public void provideFieldsFT() throws UnsupportedException, ConnectorException {

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        // table1
        assertEquals("The table must have 3 columns", 3, tableWithIndexProvided.getColumns().size());

        assertTrue("The column _id has not been found",
                        tableWithIndexProvided.getColumns().containsKey(new ColumnName(tableMetadata.getName(), "_id")));

        assertTrue("The column " + COLUMN_1 + " has not been found",
                        tableWithIndexProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadata.getName(), COLUMN_1)));
        assertTrue("The column " + COLUMN_2 + " has not been found",
                        tableWithIndexProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadata.getName(), COLUMN_2)));
        // basic table
        assertEquals("The table must have 2 columns", 2, tableSimpleProvided.getColumns().size());
        assertTrue("The column " + COLUMN_1 + " has not been found",
                        tableWithIndexProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadata.getName(), COLUMN_1)));

        assertTrue("The column _id has not been found",
                        tableSimpleProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadataSecondary.getName(), "_id")));

        assertTrue("The column _id has not been found",
                        tableSimpleProvided.getColumns().containsKey(
                                        new ColumnName(tableMetadataSecondary.getName(), SECOND_TABLE_COLUMN)));

    }

    @Test
    @Override
    public void provideCatalogMetadataNamesFT() throws UnsupportedException, ConnectorException {

        assertEquals("The catalog name is not the expected", CATALOG, catalogMetadataProvided.getName().getName());
        // 2 created collection and the system.index collection
        assertEquals("There should be 3 tables", 3, catalogMetadataProvided.getTables().size());

        TableMetadata tableWithIndexProvided = catalogMetadataProvided.getTables().get(tableMetadata.getName());
        TableMetadata tableSimpleProvided = catalogMetadataProvided.getTables().get(tableMetadataSecondary.getName());

        // Results verification
        assertTrue("The table " + tableMetadata.getName() + " should be retrieved", tableWithIndexProvided.getName()
                        .getName().equals(tableMetadata.getName().getName()));
        assertTrue("The cluster name is not the expected",
                        tableWithIndexProvided.getClusterRef().getName().equals(getClusterName().getName()));

        assertTrue("The table " + tableMetadataSecondary.getName() + " should be retrieved", tableSimpleProvided
                        .getName().getName().equals(tableMetadataSecondary.getName().getName()));
        assertTrue("The cluster name is not the expected",
                        tableSimpleProvided.getClusterRef().getName().equals(getClusterName().getName()));

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
