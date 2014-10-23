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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * The Class SelectorOptionsUtils.
 *
 * @author david
 */
public final class SelectorOptionsUtils {

    private SelectorOptionsUtils() {
    }

    /**
     * Process the options to return a map with string key.
     *
     * @param options
     *            the options. the key has to be a StringSelector
     * @return the processed options
     * @throws MongoValidationException
     *             if the options cannot be processed
     */
    public static Map<String, Selector> processOptions(Map<Selector, Selector> options) throws MongoValidationException {
        Map<String, Selector> stringOptions = null;
        if (options != null) {
            stringOptions = new HashMap<String, Selector>();
            Selector leftSelector;
            for (Entry<Selector, Selector> e : options.entrySet()) {
                leftSelector = e.getKey();
                if (leftSelector instanceof StringSelector) {
                    stringOptions.put(((StringSelector) e.getKey()).getValue(), e.getValue());
                } else {
                    throw new MongoValidationException("The left term must be a string selector");
                }
            }
        }
        return stringOptions;
    }
}
