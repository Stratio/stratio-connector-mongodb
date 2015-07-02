package com.stratio.connector.mongodb.core.configuration;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by jmgomez on 2/07/15.
 */
public class ConfigurationManagerTest {


    public static final String HOST_VALUE = "A_HOST";

    @Test
    public void testRecoverConfigurationValueDefault() throws ConnectionException {
        ConnectorClusterConfig connectorClusterConfig = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), Collections.EMPTY_MAP,Collections.EMPTY_MAP);

        ConfigurationManager configurationManager = new ConfigurationManager(connectorClusterConfig);
        String[] value =  configurationManager.recoverConfigurationValue(ConfigurationOptions.HOST);

        assertEquals("The value must be one",1,value.length);
        assertEquals("The value must be the default value",ConfigurationOptions.HOST.getDefaultValue()[0],value[0]);
    }



    @Test
    public void testRecoverConfigurationValueSet() throws ConnectionException {
        Map<String, String> clusterOption = new HashMap<>();
        clusterOption.put(ConfigurationOptions.HOST.getOptionName(), HOST_VALUE);
        ConnectorClusterConfig connectorClusterConfig = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), Collections.EMPTY_MAP,clusterOption);

        ConfigurationManager configurationManager = new ConfigurationManager(connectorClusterConfig);
        String[] value =  configurationManager.recoverConfigurationValue(ConfigurationOptions.HOST);

        assertEquals("The value must be one",1,value.length);
        assertEquals("The value must be the set value",HOST_VALUE,value[0]);
    }


    @Test
    public void testRecoverConfigurationValueDefaultConfigSet() throws ConnectionException {
        Map<String, String> clusterOption = new HashMap<>();
        clusterOption.put(ConfigurationOptions.HOST.getOptionName(), HOST_VALUE);
        ConnectorClusterConfig connectorClusterConfig = new ConnectorClusterConfig(new ClusterName("CLUSTER_NAME"), Collections.EMPTY_MAP,clusterOption);

        ConfigurationManager configurationManager = new ConfigurationManager(connectorClusterConfig);
        String[] value =  configurationManager.recoverConfigurationValue(ConfigurationOptions.PORT);

        assertEquals("The value must be one",1,value.length);
        assertEquals("The value must be the default value",ConfigurationOptions.PORT.getDefaultValue()[0],value[0]);
    }

}



