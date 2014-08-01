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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.connector.IConfiguration;

/**
 * Created by jmgomez on 14/07/14.
 */
public class ConfigurationImplem implements IConfiguration{

    private static Map<String,String> config;
    
    static{
    	        config = new HashMap<String, String>();
    	        config.put("mongo.acceptableLatencyDifference","15");
    	        config.put("mongo.maxConnectionsPerHost","100");
    	        config.put("mongo.connectTimeout","10000");
    	        config.put("mongo.maxConnectionIdleTime","0");
    	        config.put("mongo.readPreference","primaryPreferred");
    	        config.put("mongo.writeConcern","acknowledged");
    }
 
    private static List<String> seeds;
    
    static{
    	seeds = Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019");
    }
    
    //Credentials...
    
    public String getProperty(String key){

        return config.get(key);
    }

    public boolean exist(String key){
                return config.containsKey(key);
    }
    
    public List<String> getSeeds(){
		return seeds;
    }
    
    
}
