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

package com.stratio.connector.meta;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
