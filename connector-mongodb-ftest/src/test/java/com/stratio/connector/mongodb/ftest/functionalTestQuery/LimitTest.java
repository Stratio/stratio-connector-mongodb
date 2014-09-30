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

package com.stratio.connector.mongodb.ftest.functionalTestQuery;

import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.functionalTestQuery.GenericLimitTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.InitializationException;

public class LimitTest extends GenericLimitTest {

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

}

//
// public class LimitTest extends ConnectionTest {
//
// public static final String COLUMN_TEXT = "text";
// public static final String COLUMN_AGE = "age";
// public static final String COLUMN_MONEY = "money";
//
// @Test
// public void limitTest() throws Exception {
//
// insertRow(1, "text", 10, 20);// row,text,money,age
// insertRow(2, "text", 9, 17);
// insertRow(3, "text", 11, 26);
// insertRow(4, "text", 10, 30);
// insertRow(5, "text", 20, 42);
//
// LogicalPlan logicalPlan = createLogicalPlan(2);
//
// // limit 2
// MongoResultSet queryResult = (MongoResultSet) ((MongoQueryEngine) stratioMongoConnector
// .getQueryEngine()).execute(logicalPlan);
//
// assertEquals(2, queryResult.size());
//
// }
//
// private LogicalPlan createLogicalPlan(int limit) {
//
// List<LogicalStep> stepList = new ArrayList<>();
//
// List<ColumnMetadata> columns = new ArrayList<>();
//
// columns.add(new ColumnMetadata(COLLECTION, COLUMN_TEXT));
// columns.add(new ColumnMetadata(COLLECTION, COLUMN_AGE));
// Project project = new Project(CATALOG, COLLECTION, columns);
// stepList.add(project);
//
// stepList.add(new Limit(limit));
//
// return new LogicalPlan(stepList);
//
// }
//
// private void insertRow(int ikey, String texto, int money, int age)
// throws MongoException {
//
// DBCollection collection = mongoClient.getDB(CATALOG).getCollection(
// COLLECTION);
// collection.insert(BasicDBObjectBuilder.start()
// .append(COLUMN_TEXT, texto + ikey).append(COLUMN_MONEY, money)
// .append(COLUMN_AGE, age).get());
//
// }
//
// }