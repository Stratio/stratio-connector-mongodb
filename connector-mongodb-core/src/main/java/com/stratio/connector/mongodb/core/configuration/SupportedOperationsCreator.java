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


import java.util.EnumMap;
import java.util.Map;

import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.Operations;

/**
 * Created by darroyo on 9/07/14.
 */
public class SupportedOperationsCreator {

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
    public static Map<Operations, Boolean> getSupportedOperations() {
        return support;
    }



}
