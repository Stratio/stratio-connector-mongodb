//package com.stratio.connector.mongo.core; 
//
//
//import com.stratio.connector.meta.ConfigurationImplem;
//import com.stratio.connector.mongodb.core.MongoConfiguration;
//import com.stratio.meta.common.connector.IConfiguration;
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import static org.mockito.Mockito.*;
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mock.*;
//import static org.junit.Assert.*;
///**
//* Policy Tester. 
//* 
//* @author <Authors name> 
//* @since <pre>jul 14, 2014</pre> 
//* @version 1.0 
//*/
//@RunWith(PowerMockRunner.class)
//
//
//public class PolicyTest { 
//
//
//
//
//
//    @Test
//    public void testMongoClientOptions() throws Exception {
//        MongoConfiguration policy = new MongoConfiguration(new ConfigurationImplem());
//        assertEquals("The property connectionPerHost is correct",100,policy.getMongoClientOptions().getConnectionsPerHost());
//
//
//
//    }
//    /**
//* 
//* Method: getClientPolicy() 
//* 
//*/ 
//@Test
//public void testGetClientPolicy() throws Exception {
//    Policy policy = new MongoConfiguration(new ConfigurationImplem());
//    ClientPolicy clientPolicy = policy.getClientPolicy();
//
//    assertEquals("The property maxSocketIdle match with test/resources/aerspikePolicy.propertis",15,clientPolicy.maxSocketIdle);
//    assertEquals("The property maxThreads match with test/resources/aerspikePolicy.propertis",5,clientPolicy.maxThreads);
//    assertEquals("The property timeout match with test/resources/aerspikePolicy.propertis",145673,clientPolicy.timeout);
//    assertEquals("The property failIfNotConnected match with test/resources/aerspikePolicy.propertis", false, clientPolicy.failIfNotConnected);
//} 
//
//
//} 
