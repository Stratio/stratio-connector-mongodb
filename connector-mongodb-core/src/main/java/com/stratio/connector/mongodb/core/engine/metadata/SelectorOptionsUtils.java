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
package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * @author david
 *
 */
public class SelectorOptionsUtils {

    private SelectorOptionsUtils() {
    }

    /**
     * @param options
     * @return
     * @throws MongoValidationException 
     */
    public static Map<String, Selector> processOptions(Map<Selector, Selector> options) throws MongoValidationException {
    	Map<String, Selector> stringOptions = null;
		if (options != null) {
			stringOptions = new HashMap<String, Selector>();
			Selector leftSelector;
			for (Entry<Selector, Selector> e : options.entrySet()) {
				leftSelector = e.getKey();
				if (leftSelector instanceof StringSelector) {
					stringOptions.put(((StringSelector) e.getKey()).getValue(),
							e.getValue());
				} else
					throw new MongoValidationException(
							"The left term must be a string selector");
			}
		}
		return stringOptions;
	}
}
