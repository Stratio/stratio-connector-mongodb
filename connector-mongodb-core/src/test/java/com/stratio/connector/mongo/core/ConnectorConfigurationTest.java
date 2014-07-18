//package com.stratio.connector.mongo.core;
//
//import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.AerospikeException;
//import com.aerospike.client.Bin;
//import com.aerospike.client.Key;
//import com.aerospike.client.policy.WritePolicy;
//import com.stratio.connector.aerospike.core.engine.AerospikeStorageEngine;
//import com.stratio.connector.meta.ConfigurationImpl;
//import com.stratio.connector.meta.IColumn;
//import com.stratio.connector.meta.IRow;
//
//import com.stratio.connector.meta.Operation;
//
//
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import static org.powermock.api.mockito.PowerMockito.*;
//import static org.mockito.Mockito.any;
//
//import java.lang.reflect.Field;
//import java.util.HashSet;
//import java.util.Map;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
///**
// * ConfigurationConfiguration Tester.
// *
// * @author <Authors name>
// * @version 1.0
// * @since <pre>jul 9, 2014</pre>
// */
//@RunWith(PowerMockRunner.class)
//@PrepareForTest(value = {AerospikeConnectionSupportOperation.class, AerospikeClient.class, AerospikeStorageEngine.class})
//public class ConnectorConfigurationTest {
//
//
//
//
//    /**
//     * Method: getOperation(Operation operation)
//     */
//    @Test
//    public void testAllOperationSupport() throws Exception {
//        AerospikeConnectionSupportOperation connectorConfiguration = new AerospikeConnectionSupportOperation();
//        Field f = connectorConfiguration.getClass().getDeclaredField("support");
//        f.setAccessible(true);
//
//        Map support = (Map) f.get(connectorConfiguration);
//        assertEquals("All operation have configuration", support.size(), Operation.values().length - 1);
//    }
//
//    @Test
//    public void InsertCoherence() throws AerospikeException {
//
//        AerospikeConnectionSupportOperation connectorConfiguration = new AerospikeConnectionSupportOperation();
//        AerospikeStorageEngine aerospikeStorageEngine = new AerospikeStorageEngine(new Policy(new ConfigurationImpl()));
//        AerospikeClient aerospikeClient = mock(AerospikeClient.class);
//        aerospikeStorageEngine.setConnection(aerospikeClient);
//        IRow iRow = creteRow();
//        doNothing().when(aerospikeClient).add(any(WritePolicy.class),any(Key.class), any(Bin[].class));
//        if (connectorConfiguration.getOperation(Operation.INSERT)){
//            try {
//                aerospikeStorageEngine.insert("keyspace", "set",  iRow);
//            } catch (Exception e) {
//                fail("The operation Insert (Row) is supported");
//            }
//            try {
//                aerospikeStorageEngine.insert("keyspace", "set", new HashSet<IRow>());
//            } catch (Exception e) {
//                fail("The operation Insert (Set<Row>) is supported");
//            }
//        }else{
//            try {
//                aerospikeStorageEngine.insert("keyspace", "set",  iRow);
//                fail("The operation Insert (Row) is not supported");
//            } catch (Exception e) {
//
//            }
//            try {
//                aerospikeStorageEngine.insert("keyspace", "set", new HashSet<IRow>());
//                fail("The operation Insert (Set<Row>) is not supported");
//            } catch (Exception e) {
//
//            }
//        }
//
//    }
//
//    private IRow creteRow() {
//        IRow iRow = mock(IRow.class);
//        IColumn iColumn =mock(IColumn.class);
//        when(iColumn.getName()).thenReturn("name");
//        when(iColumn.getValue()).thenReturn("value");
//        when(iRow.getPk()).thenReturn(iColumn);
//
//        return iRow;
//    }
//
//
//} 
