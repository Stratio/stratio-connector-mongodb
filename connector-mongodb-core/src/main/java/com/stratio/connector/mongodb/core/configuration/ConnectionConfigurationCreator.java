package com.stratio.connector.mongodb.core.configuration;


import java.util.HashSet;
import java.util.Set;

import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.ConnectionOption;

/**
* Created by jmgomez on 22/07/14.
*/
public class ConnectionConfigurationCreator {


    private static Set<ConnectionConfiguration> configuration = new HashSet<>();

    static {
        configuration.add(new ConnectionConfiguration(ConnectionOption.HOST_IP, true, true));
        configuration.add(new ConnectionConfiguration(ConnectionOption.HOST_PORT, false, true));
    }

    public static Set<ConnectionConfiguration> getConfiguration() {
        return configuration;
    }
}