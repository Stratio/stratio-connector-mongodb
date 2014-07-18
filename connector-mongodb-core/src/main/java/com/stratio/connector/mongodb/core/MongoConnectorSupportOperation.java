package com.stratio.connector.mongodb.core;


import java.util.EnumMap;
import java.util.Map;

import com.stratio.connector.meta.IConnectionSupportOperation;
import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.Operations;

/**
 * Created by darroyo on 9/07/14.
 */
public class MongoConnectorSupportOperation implements IConnectionSupportOperation {

    private static final Map<Operations, Boolean> support;

    static {
        support = new EnumMap<Operations, Boolean>(Operations.class);
        support.put(Operations.CREATE_CATALOG, Boolean.TRUE);
        support.put(Operations.CREATE_TABLE, Boolean.TRUE);
        support.put(Operations.DELETE, Boolean.TRUE);
        support.put(Operations.DROP_CATALOG, Boolean.TRUE);
        support.put(Operations.DROP_TABLE, Boolean.TRUE);
        support.put(Operations.INSERT, Boolean.TRUE);
		//support.put(Operations.INSERT_BULK, Boolean.TRUE);
        support.put(Operations.SELECT_AGGREGATION_SELECTORS, Boolean.FALSE);
        support.put(Operations.SELECT_GROUP_BY, Boolean.FALSE);
        support.put(Operations.SELECT_INNER_JOIN, Boolean.FALSE);
        support.put(Operations.SELECT_LIMIT, Boolean.TRUE);
        support.put(Operations.SELECT_ORDER_BY, Boolean.TRUE);
        support.put(Operations.SELECT_WHERE_BETWEEN, Boolean.TRUE);
        support.put(Operations.SELECT_WHERE_MATCH, Boolean.TRUE);
        support.put(Operations.SELECT_WINDOW, Boolean.FALSE);
    }

    /**
     * indicates if operation is supported
     *
     * @param operation the operation.
     * @return true if the operation is supported. False in other case.
     */
    public Boolean getOperation(Operations operation) {
        return support.get(operation);
    }



}
