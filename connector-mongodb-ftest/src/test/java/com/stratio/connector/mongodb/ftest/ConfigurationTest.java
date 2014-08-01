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

package com.stratio.connector.mongodb.ftest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.ConnectionOption;
import com.stratio.meta.common.connector.Operations;




public class ConfigurationTest extends ConnectionTest {



    @Test
    public void supportedOperationsTest() throws Exception {

    	assertTrue(stratioMongoConnector.getSupportededOperations().containsKey(Operations.CREATE_CATALOG));
    	assertTrue("insert is supported",stratioMongoConnector.getSupportededOperations().get(Operations.INSERT));

    }

    @Test
    public void connectionConfigurationTest() throws Exception {
    	Set<ConnectionConfiguration> options =  stratioMongoConnector.getConnectionConfiguration();
    	int numHostOption = 0;
    	for(ConnectionConfiguration option: options){
    		if (option.getConnectionOption() == ConnectionOption.HOST_IP) numHostOption++;
    	}
    	assertEquals( 1, numHostOption );


    }
 
 
}