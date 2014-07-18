package com.stratio.connector.meta;

import com.stratio.meta.common.connector.IConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    	Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019");
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
