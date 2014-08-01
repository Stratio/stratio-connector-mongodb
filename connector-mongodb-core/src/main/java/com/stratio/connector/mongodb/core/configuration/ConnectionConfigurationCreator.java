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

    /**
	 * Return the connectionConfiguration options.
	 * 
	 * @return the connectionConfiguration options.
	 */
    public static Set<ConnectionConfiguration> getConfiguration() {
        return configuration;
    }
}