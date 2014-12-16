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
package com.stratio.connector.mongodb.ftest.functionalTestQuery;

import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.functionalTestQuery.GenericQueryFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryFT extends GenericQueryFT {

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
