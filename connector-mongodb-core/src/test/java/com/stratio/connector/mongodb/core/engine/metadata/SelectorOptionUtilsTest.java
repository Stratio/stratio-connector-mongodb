/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.mongodb.core.engine.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.statements.structures.selectors.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.selectors.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;
import com.stratio.crossdata.common.statements.structures.selectors.StringSelector;

/**
 * @author david
 */
public class SelectorOptionUtilsTest {

    private static final String OPTION_NAME = "option";
    private static final String OPTION_NAME2 = "option2";
    private static final Integer INT_VALUE = 5;
    private static final Boolean BOOLEAN_VALUE = true;

    @Test
    public void processOptionsTest() throws MongoValidationException {

        Map<Selector, Selector> options = new java.util.HashMap<Selector, Selector>();
        options.put(new StringSelector(OPTION_NAME), new IntegerSelector(INT_VALUE));
        options.put(new StringSelector(OPTION_NAME2), new BooleanSelector(BOOLEAN_VALUE));

        Map<String, Selector> processedOptions = SelectorOptionsUtils.processOptions(options);

        assertTrue(processedOptions.containsKey(OPTION_NAME));
        assertTrue(processedOptions.containsKey(OPTION_NAME2));
        assertEquals(INT_VALUE.longValue(), ((IntegerSelector) processedOptions.get(OPTION_NAME)).getValue());
        assertEquals(BOOLEAN_VALUE, ((BooleanSelector) processedOptions.get(OPTION_NAME2)).getValue());

    }

    @Test(expected = MongoValidationException.class)
    public void processMalformedOptionsTest() throws MongoValidationException {

        Map<Selector, Selector> options = new java.util.HashMap<Selector, Selector>();
        options.put(new IntegerSelector(INT_VALUE), new IntegerSelector(INT_VALUE));

        SelectorOptionsUtils.processOptions(options);

    }
}
