//package com.stratio.connector.mongo.core.engine; 
//
//import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.AerospikeException;
//import com.aerospike.client.Bin;
//import com.aerospike.client.Key;
//
//import com.aerospike.client.policy.WritePolicy;
//import com.stratio.connector.aerospike.core.Policy;
//import com.stratio.connector.meta.ConfigurationImpl;
//import com.stratio.connector.meta.IColumn;
//import com.stratio.connector.meta.IRow;
//
//import com.stratio.meta.common.exceptions.ExecutionException;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Matchers;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//
//import java.lang.reflect.Field;
//import java.util.HashSet;
//import java.util.Random;
//import java.util.Set;
//
//
//import static org.mockito.Mockito.eq;
//import static org.mockito.Mockito.*;
//import static org.mockito.Mockito.when;
//import static org.powermock.api.mockito.PowerMockito.mock;
//
//
///**
//* AerospikeStorageEngine Tester. 
//* 
//* @author <Authors name> 
//* @since <pre>jul 10, 2014</pre> 
//* @version 1.0 
//*/
//@RunWith(PowerMockRunner.class)
//
//@PrepareForTest(value = {AerospikeStorageEngine.class, AerospikeClient.class})
//public class AerospikeStorageEngineTest {
//
//
//    public static final String NAMESPACE = "catalog";
//    public static final String SET_NAME = "tableName";
//    private int MAX_ROWS = 2;
//
//    /**
//* 
//* Method: insert(String catalog, String tableName, iRow row) 
//* 
//*/ 
//@Test
//public void testInsertOne() throws Exception {
//    AerospikeClient mockAerispikeClient = createAeropikeClientMock();
//    AerospikeStorageEngine aerospikeStorageEngine = createAerospikeStorageEngine(mockAerispikeClient);
//    IColumn mockColumnPk = createColumn("PKName", "PKValue");
//    Set<IColumn> columns = createColumns();
//    IRow iRow = createIRowMock(mockColumnPk,columns);
//
//
//    aerospikeStorageEngine.insert(NAMESPACE, SET_NAME,iRow);
//
//    Key key = new Key(NAMESPACE, SET_NAME,mockColumnPk.getValue());
//    verify(mockAerispikeClient).add(any(WritePolicy.class),eq(key),(Bin)anyObject(),(Bin)anyObject());
//
//}
//
//
//    /*@Test(expected = ExecutionException.class)
//    public void testInsertOneExcutionException() throws ExecutionException, AerospikeException {
//        AerospikeClient mockAerispikeClient = createAeropikeClientExecutionExceptionMock();
//        AerospikeStorageEngine aerospikeStorageEngine = createAerospikeStorageEngine(mockAerispikeClient);
//        IColumn mockColumnPk = createColumn("PKName", "PKValue");
//        Set<IColumn> columns = createColumns();
//        IRow iRow = createIRowMock(mockColumnPk,columns);
//
//
//        aerospikeStorageEngine.insert(NAMESPACE, SET_NAME,iRow);
//
//
//
//    }*/
//
//
//
//    /**
//* 
//* Method: insert(String catalog, String tableName, Set<iRow> rows) 
//* 
//*/ 
//@Test
//public void testInsertBulk() throws Exception {
//
//    AerospikeClient mockAerispikeClient = createAeropikeClientMock();
//    AerospikeStorageEngine aerospikeStorageEngine = createAerospikeStorageEngine(mockAerispikeClient);
//    Set<IRow> rows = createRows();
//    addRandomGenerator(aerospikeStorageEngine);
//
//    aerospikeStorageEngine.insert(NAMESPACE, SET_NAME,rows);
//
//    Key key = new Key(NAMESPACE, SET_NAME,new Long(1));
//    verify(mockAerispikeClient).put(any(WritePolicy.class), Matchers.eq(key), (Bin) anyObject(), (Bin) anyObject(), (Bin) anyObject(), (Bin) anyObject());
//
//}
//
//    private void addRandomGenerator(AerospikeStorageEngine aerospikeStorageEngine) throws NoSuchFieldException, IllegalAccessException {
//        Field field = AerospikeStorageEngine.class.getDeclaredField("random");
//        field.setAccessible(true);
//        Random ramdomMock = mock(Random.class);
//        when(ramdomMock.nextLong()).thenReturn(new Long(1));
//        field.set(aerospikeStorageEngine, ramdomMock);
//    }
//
//
//    /**
//* 
//* Method: delete(String catalog, String tableName, IFilter filter) 
//* 
//*/ 
//@Test
//public void testDelete() throws Exception { 
////TODO: Test goes here... 
//}
//
//
//    private AerospikeClient createAeropikeClientMock() throws AerospikeException {
//        AerospikeClient mockAerispikeClient = mock(AerospikeClient.class);
//        doNothing().when(mockAerispikeClient).add(any(WritePolicy.class),any(Key.class),any(Bin[].class));
//        doNothing().when(mockAerispikeClient).put(any(WritePolicy.class), any(Key.class), any(Bin[].class));
//        return mockAerispikeClient;
//    }
//
//    private AerospikeClient createAeropikeClientExecutionExceptionMock() throws AerospikeException {
//        AerospikeClient mockAerispikeClient = mock(AerospikeClient.class);
//        doThrow(new AerospikeException()).when(mockAerispikeClient).add(any(WritePolicy.class),any(Key.class),any(Bin[].class));
//        //doThrow(new AerospikeException()).when(mockAerispikeClient).put(any(WritePolicy.class),any(Key.class),any(Bin[].class));
//        return mockAerispikeClient;
//    }
//
//
//
//    private Set<IColumn> createColumns() {
//        Set<IColumn> columns = new HashSet<IColumn>();
//        columns.add(createColumn("Field1","Value1"));
//        columns.add(createColumn("Field2","Value2"));
//        return columns;
//    }
//
//    private IRow createIRowMock(IColumn mockColumnPk, Set<IColumn> columns) {
//        IRow iRow = mock(IRow.class);
//        when(iRow.getPk()).thenReturn(mockColumnPk);
//        when(iRow.getColumns()).thenReturn(columns);
//        return iRow;
//    }
//
//    private IColumn createColumn(String name, String value) {
//        IColumn mockColumnPk = mock(IColumn.class);
//        when(mockColumnPk.getName()).thenReturn(name);
//        when(mockColumnPk.getValue()).thenReturn(value);
//        return mockColumnPk;
//    }
//
//
//
//    private Set<IRow> createRows() {
//        Set<IRow> rows = new HashSet<>();
//        for (int i=0;i<MAX_ROWS;i++){
//
//            IColumn mockColumnPk = createColumn("PKName"+i, "PKValue"+i);
//            Set<IColumn> columns = createColumns();
//            IRow iRow = createIRowMock(mockColumnPk, columns);
//            rows.add(iRow);
//        }
//        return rows;
//    }
//
//    private AerospikeStorageEngine createAerospikeStorageEngine(AerospikeClient mockAerispikeClient) {
//        AerospikeStorageEngine aerospikeStorageEngine = new AerospikeStorageEngine(new Policy(new ConfigurationImpl()));
//        aerospikeStorageEngine.setConnection(mockAerispikeClient);
//        return aerospikeStorageEngine;
//    }
//} 
