package com.stratio.connector.mongodb.core.configuration;

import com.stratio.connector.commons.util.PropertyValueRecovered;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class must managed the configuration.
 * Created by jmgomez on 2/07/15.
 */
public class ConfigurationManager {


    /**
     * The cluster configuration.
     *
     */
    private ConnectorClusterConfig configuration;

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     * * @param configuration the cluster configuration.
     */
    public ConfigurationManager(ConnectorClusterConfig configuration) {
        this.configuration = configuration;
    }

    /**
     * Recovered the option value or the default value if the property is not set.
     * @param configurationOption the configuration option.
     * @return the configuration option value.
     * @throws ConnectionException if the property is not set.
     */
    public String[] recoverConfigurationValue( ConfigurationOptions configurationOption) throws ConnectionException {
        String[] optionValue = {};
        try {
            Map<String, String> config = configuration.getClusterOptions();
            if (config != null) {
                String strPorts = config.get(configurationOption.getOptionName());
                if (strPorts != null) {
                    optionValue = PropertyValueRecovered.recoveredValueASArray(String.class, strPorts);
                } else {
                    optionValue = configurationOption.getDefaultValue();
                }
            } else {
                optionValue = configurationOption.getDefaultValue();
            }
        }catch(ExecutionException e){
            String msg = String.format("Error recovering [%s] option ",configurationOption.getOptionName());
            logger.error(msg);
            throw new ConnectionException(msg,e);
        }
        return optionValue;
    }
}
