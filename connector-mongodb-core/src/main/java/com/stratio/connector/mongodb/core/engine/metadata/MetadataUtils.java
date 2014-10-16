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

import com.stratio.meta2.common.statements.structures.selectors.Selector;

/**
 * @author david
 *
 */
public class MetadataUtils {

    private MetadataUtils() {
    }

    /**
     * @param options
     * @return
     */
    public static Map<String, Selector> processOptions(Map<Selector, Selector> options) {
        Map<String, Selector> stringOptions = new HashMap<String, Selector>();

        for (Entry<Selector, Selector> e : options.entrySet()) {
            stringOptions.put(e.getKey().getAlias(), e.getValue());
        }
        return stringOptions;
    }
}
