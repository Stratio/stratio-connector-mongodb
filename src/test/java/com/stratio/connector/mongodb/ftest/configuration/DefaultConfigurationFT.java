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

package com.stratio.connector.mongodb.ftest.configuration;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.connector.mongodb.ftest.helper.DefaultConfigurationMongoConnectorHelper;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;

public class DefaultConfigurationFT extends GenericConnectorTest<MongoConnector> {

    /*
     * (non-Javadoc)
     * 
     * @see com.stratio.connector.commons.ftest.GenericConnectorTest#getConnectorHelper()
     */
    @Override
    protected IConnectorHelper getConnectorHelper() {

        DefaultConfigurationMongoConnectorHelper mongoConnectorHelper = null;
        try {
            mongoConnectorHelper = new DefaultConfigurationMongoConnectorHelper(getClusterName());
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        }
        return mongoConnectorHelper;
    }

    @Test
    public void defaultConfig() {

        assertTrue("It is possible connect without optional properties", getConnector() != null);
    }
}