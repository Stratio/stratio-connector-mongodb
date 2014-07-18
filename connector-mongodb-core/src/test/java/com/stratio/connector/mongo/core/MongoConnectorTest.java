//package com.stratio.connector.mongo.core;
//
//import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.AerospikeException;
//import com.aerospike.client.policy.ClientPolicy;
//import com.mongodb.MongoClient;
//import com.stratio.connector.aerospike.core.engine.AerospikeMetaProvider;
//import com.stratio.connector.aerospike.core.engine.AerospikeQueryEngine;
//import com.stratio.connector.aerospike.core.engine.AerospikeStorageEngine;
//import com.stratio.connector.meta.ConfigurationImpl;
//import com.stratio.connector.meta.ConfigurationImplem;
//import com.stratio.connector.mongodb.core.MongoConnector;
//import com.stratio.meta.common.connector.IConfiguration;
//import com.stratio.meta.common.connector.IQueryEngine;
//import com.stratio.meta.common.connector.IStorageEngine;
//import com.stratio.meta.common.exceptions.ConnectionException;
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.lang.reflect.Field;
//
//import static org.junit.Assert.*;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.*;
//
//
///**
// * AerospikeConnector Tester.
// *
// * @author <Authors name>
// * @version 1.0
// * @since <pre>jul 8, 2014</pre>
// */
//@RunWith(PowerMockRunner.class)
//
//@PrepareForTest(value = {MongoConnector.class, MongoClient.class})
//
//public class MongoConnectorTest {
//
//
//    @Test
//    public void connectTest() throws Exception, ConnectionException {
//    	MongoConnector connector = new MongoConnector();
//    	MongoClient mongoClient = configureMongoClientCreation(true);
//    	MongoMetaProvider mongoMetaProvider = returnMetaProvider(connector);
//    	MongoQueryEngine mongoQueryEngine =  returnQueryEngine(connector);
//    	MongoStorageEngine mongoStorageEngine =  returnStorageEngine(connector);
//
//        connector.getMedatadaProvider();
//        connector.getQueryEngine();
//        connector.getStorageEngine();
//
//
//        connector.init(null,new ConfigurationImplem());
//
//        ensureConnectionPropagation(mongoClient, mongoMetaProvider, mongoQueryEngine, mongoStorageEngine);
//
////        verifyNew(MongoClient.class).withArguments(any(ClientPolicy.class),eq("host"), eq(3000));
//        assertNotNull("The mongoClient is not null", getPrivateField(connector, "client"));
//
//    }
//
//    @Test(expected=ConnectionException.class)
//    public void reConnectExceotionTest() throws Exception, ConnectionException {
//        AerospikeConnector connector = new AerospikeConnector();
//        AerospikeClient aerospikeClient = configureAerospikeClientCreation(true);
//        AerospikeMetaProvider aerospikeMetaProvider = returnMetaProvider(connector);
//        AerospikeQueryEngine aerospikeQueryEngine =  returnQueryEngine(connector);
//        AerospikeStorageEngine aerospikeStorageEngine =  returnStorageEngine(connector);
//
//
//
//        connector.connect(null,null);
//        connector.connect(null,null);
//
//    }
//
//
//    @Test(expected=ConnectionException.class)
//    public void noConfigurationExceotionTest() throws Exception, ConnectionException {
//        AerospikeConnector connector = new AerospikeConnector();
//        connector.connect(null,null);
//
//
//    }
//
//
//    private void ensureConnectionPropagation(AerospikeClient aerospikeClient, AerospikeMetaProvider aerospikeMetaProvider, AerospikeQueryEngine aerospikeQueryEngine, AerospikeStorageEngine aerospikeStorageEngine) {
//        verify(aerospikeMetaProvider).setConnection(aerospikeClient);
//        verify(aerospikeQueryEngine).setConnection(aerospikeClient);
//        verify(aerospikeStorageEngine).setConnection(aerospikeClient);
//    }
//
//    @Test(expected = ConnectionException.class)
//    public void connectExceptionTest() throws Exception, ConnectionException {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreationException();
//
//        connector.connect(null,null);
//
//    }
//
//
//    @Test
//    public void connectWirhCredentialsTest() throws Exception, ConnectionException {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//
//        connector.connect(null,new ConfigurationImpl());
//
//        verifyNew(AerospikeClient.class).withArguments(any(ClientPolicy.class),eq("host"), eq(3000));
//        assertNotNull("The aereospikeClient is not null", getPrivateField(connector, "client"));
//
//    }
//
//    @Test(expected = ConnectionException.class)
//    public void connectWithCredentialsExceptionTest() throws Exception, ConnectionException {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreationException();
//
//        connector.connect(null,new ConfigurationImpl());
//
//    }
//
//
//
//    @Test
//    public void configurationTest() {
//        AerospikeConnector connector = new AerospikeConnector();
//        AerospikeConnectionSupportOperation configurationConfiguration = connector.getConfiguration();
//        AerospikeConnectionSupportOperation otherConfigurationConfiguration = connector.getConfiguration();
//
//        assertNotNull("The configuration is not null",configurationConfiguration);
//        assertSame("The Configuration is singleton", configurationConfiguration, otherConfigurationConfiguration);
//    }
//
//
//
//    @Test
//    public void closeTest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        AerospikeClient mockAerospikeClient = configureAerospikeClientCreation(true);
//
//        configAerospikeClient(connector, mockAerospikeClient);
//
//        connector.close();
//
//        verify(mockAerospikeClient).close();
//
//    }
//
//
//    @Test
//    public void cleanContextTest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//        connector.connect(null,new ConfigurationImpl());
//
//        connector.close();
//
//        assertNull("The aerospikeClient is null", getPrivateField(connector, "client"));
//
//
//
//    }
//
//    @Test
//    public void getStorageEngineTest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//        connector.connect(null,new ConfigurationImpl());
//        AerospikeStorageEngine aerospikeStorageEngine = returnStorageEngine(connector);
//        verify(aerospikeStorageEngine).setConnection(any(AerospikeClient.class));
//        IStorageEngine OtherIStorageEngine = returnStorageEngine(connector);
//
//        assertNotNull("The storageEngine is not null", aerospikeStorageEngine);
//        assertSame("The storageEngine is singleton", aerospikeStorageEngine, OtherIStorageEngine);
//
//
//    }
//
//
//
//
//    @Test
//    public void getQueryEngineTest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//        connector.connect(null,new ConfigurationImpl());
//        AerospikeQueryEngine queryEngine = returnQueryEngine(connector);
//        verify(queryEngine).setConnection(any(AerospikeClient.class));
//        IQueryEngine OtherIQueryEngine = returnQueryEngine(connector);
//
//        assertNotNull("The QueryEngine is not null", queryEngine);
//        assertSame("The QueryEngine is singleton", queryEngine, OtherIQueryEngine);
//
//    }
//
//
//    @Test
//    public void getMetaProviderTest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//        connector.connect(null,new ConfigurationImpl());
//
//        AerospikeMetaProvider iMetadataProvider = returnMetaProvider(connector);
//        verify(iMetadataProvider).setConnection(any(AerospikeClient.class));
//
//        AerospikeMetaProvider OtherIQueryEngine = returnMetaProvider(connector);
//
//        assertNotNull("The MetadataProvider is not null", iMetadataProvider);
//        assertSame("The MetadataProvider is singleton", iMetadataProvider, OtherIQueryEngine);
//
//
//    }
//
//
//
//    @Test
//    public void getDatastoreNameTest(){
//        AerospikeConnector connector = new AerospikeConnector();
//
//        assertEquals("The DataStoren name is correct", "Aerospike", connector.getDatastoreName());
//
//    }
//
//    @Test
//    public void isConnectedtest() throws Exception {
//        AerospikeConnector connector = new AerospikeConnector();
//        configureAerospikeClientCreation(true);
//        connector.connect(null,new ConfigurationImpl());
//
//        assertTrue("The client is connected", connector.isConnected());
//    }
//
//
//    private AerospikeStorageEngine returnStorageEngine(AerospikeConnector connector) throws Exception {
//        AerospikeStorageEngine storageEngine = mock(AerospikeStorageEngine.class);
//        whenNew(AerospikeStorageEngine.class).withAnyArguments().thenReturn(storageEngine);
//        return (AerospikeStorageEngine)connector.getStorageEngine();
//    }
//
//
//
//
//    private AerospikeQueryEngine returnQueryEngine(AerospikeConnector connector) throws Exception {
//        AerospikeQueryEngine queryEngine = mock(AerospikeQueryEngine.class);
//        whenNew(AerospikeQueryEngine.class).withAnyArguments().thenReturn(queryEngine);
//        return (AerospikeQueryEngine)connector.getQueryEngine();
//    }
//
//    private AerospikeMetaProvider returnMetaProvider(AerospikeConnector connector) throws Exception {
//        AerospikeMetaProvider iMetadataProvider = mock(AerospikeMetaProvider.class);
//        whenNew(AerospikeMetaProvider.class).withNoArguments().thenReturn(iMetadataProvider);
//        iMetadataProvider = (AerospikeMetaProvider)connector.getMedatadaProvider();
//        return iMetadataProvider;
//    }
//
//
//    private Object getPrivateField(AerospikeConnector connector, String field) throws NoSuchFieldException, IllegalAccessException {
//        Field f = connector.getClass().getDeclaredField(field);
//        f.setAccessible(true);
//
//        return f.get(connector);
//    }
//
//    private AerospikeClient configureAerospikeClientCreation(boolean connected) throws Exception {
//        AerospikeClient aerospikeClient = mock(AerospikeClient.class);
//        when(aerospikeClient.isConnected()).thenReturn(connected);
//        whenNew(AerospikeClient.class).withAnyArguments().thenReturn(aerospikeClient);
//        doNothing().when(aerospikeClient).close();
//        return aerospikeClient;
//    }
//
//    private void configAerospikeClient(AerospikeConnector connector, AerospikeClient mockAerospikeClient) throws NoSuchFieldException, IllegalAccessException {
//        Field f = connector.getClass().getDeclaredField("client");
//        f.setAccessible(true);
//        f.set(connector, mockAerospikeClient);
//    }
//
//    private void configureAerospikeClientCreationException() throws Exception {
//        whenNew(AerospikeClient.class).withArguments(any(ClientPolicy.class), eq("host"), eq(3000)).thenThrow(new AerospikeException());
//    }
//
//
//}
