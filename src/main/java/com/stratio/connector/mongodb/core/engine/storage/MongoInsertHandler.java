package com.stratio.connector.mongodb.core.engine.storage;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.stratio.connector.mongodb.core.exceptions.MongoInsertException;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;

public class MongoInsertHandler {

    /** The logger. */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private DBCollection collection;
    private BulkWriteOperation bulkWriteOperation;

    public MongoInsertHandler(DBCollection collection) {
        this.collection = collection;
    }

    public void startBatch() {
        bulkWriteOperation = collection.initializeUnorderedBulkOperation();
    }

    public void insertIfNotExist(TableMetadata targetTable, Row row, String pk) throws UnsupportedException {
        // TODO insert with _id
        throw new UnsupportedException("It will be included soon");
    }

    public void upsert(TableMetadata targetTable, Row row, Object pk) throws MongoInsertException,
                    MongoValidationException {
        // Upsert searching for _id
        DBObject doc = getBSONFromRow(targetTable, row);
        BasicDBObject find = new BasicDBObject("_id", pk);

        try {
            if (bulkWriteOperation != null) {
                bulkWriteOperation.find(find).upsert().update(new BasicDBObject("$set", doc));
            } else {
                collection.update(find, new BasicDBObject("$set", doc), true, false);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Row updated with fields: " + doc.keySet());
            }
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }
    }

    public void insertWithoutPK(TableMetadata targetTable, Row row) throws MongoValidationException,
                    MongoInsertException {

        DBObject doc = getBSONFromRow(targetTable, row);
        try {
            if (bulkWriteOperation != null) {
                bulkWriteOperation.insert(doc);
            } else {
                collection.insert(doc);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Row inserted with fields: " + doc.keySet());
            }
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }

    }

    public void executeBatch() throws MongoInsertException {
        try {
            bulkWriteOperation.execute();
        } catch (MongoException e) {
            logger.error("Error inserting data: " + e.getMessage());
            throw new MongoInsertException(e.getMessage(), e);
        }
    }

    // Building the fields to insert in Mongo
    private DBObject getBSONFromRow(TableMetadata targetTable, Row row) throws MongoValidationException {

        String catalog = targetTable.getName().getCatalogName().getName();
        String tableName = targetTable.getName().getName();
        BasicDBObject doc = new BasicDBObject();
        String cellName;
        Object cellValue;

        for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
            cellName = entry.getKey();
            cellValue = entry.getValue().getValue();
            ColumnName cName = new ColumnName(catalog, tableName, cellName);
            validateDataType(targetTable.getColumns().get(cName).getColumnType());
            doc.put(entry.getKey(), cellValue);
        }
        return doc;
    }

    /**
     * Validates the data type.
     *
     * @param colType
     *            the column type
     * @param cellValue
     *            the cell value
     * @throws MongoValidationException
     *             if the type is not supported
     */
    private void validateDataType(ColumnType columnType) throws MongoValidationException {

        switch (columnType) {
        case BIGINT:
        case BOOLEAN:
        case INT:
        case TEXT:
        case VARCHAR:
        case DOUBLE:
        case FLOAT:
            break;
        case SET:
        case LIST:
            validateDataType(columnType.getDBInnerType());
            break;
        case MAP:
            validateDataType(columnType.getDBInnerType());
            validateDataType(columnType.getDBInnerValueType());
            break;
        case NATIVE:
            throw new MongoValidationException("Type not supported: " + columnType.toString());

        default:
            throw new MongoValidationException("Type not supported: " + columnType.toString());
        }

    }

}
