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