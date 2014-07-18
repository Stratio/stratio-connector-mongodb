package com.stratio.connector.meta;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.stratio.meta.common.data.CassandraResultSet;
import com.stratio.meta.common.data.Row;

public class MResultSetIterator implements Iterator<com.stratio.meta.common.data.Row> {

    /**
     * Set representing a result from Mongo.
     */
    private final MongoResultSet mResultSet;

    /**
     * Pointer to the current element.
     */
    private int current;

    /**
     * Build a {@link com.stratio.meta.common.data.CResultSetIterator} from a {@link com.stratio.meta.common.data.CassandraResultSet}.
     * @param cResultSet Cassandra Result Set.
     */
    public MResultSetIterator(MongoResultSet mResultSet) {
        this.mResultSet = mResultSet;
        this.current = 0;
    }

    @Override
    public boolean hasNext() {
        return current < mResultSet.getRows().size();
    }

    @Override
    public com.stratio.meta.common.data.Row next() throws NoSuchElementException{
        return mResultSet.getRows().get(current++);
    }

    @Override
    public void remove() throws UnsupportedOperationException, IllegalStateException{
        mResultSet.remove(current);
    }
}
