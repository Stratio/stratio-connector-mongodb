/*
* Licensed to STRATIO (C) under one or more contributor license agreements.
* See the NOTICE file distributed with this work for additional information
* regarding copyright ownership. The STRATIO (C) licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.stratio.connector.mongodb.core.configuration;

import com.stratio.meta2.common.statements.structures.selectors.SelectorType;

/**
 * @author darroyo Set of options for the mongo connector. A default value is provided.
 *
 */

public enum TableOptions {

    SHARDING_ENABLED("enable_sharding", SelectorType.BOOLEAN, false), SHARD_KEY_TYPE("shard_key_type",
                    SelectorType.STRING, ShardKeyType.ASC), SHARD_KEY_FIELDS("shard_key_fields", SelectorType.STRING,
                    new String[] { "_id" });

    private final String optionName;
    private final SelectorType selectorType;
    private final Object defaultValue;

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getOptionName() {
        return optionName;
    }

    public SelectorType getSelectorType() {
        return selectorType;
    }

    TableOptions(String optionName, SelectorType selectorType, Object defaultValue) {
        this.optionName = optionName;
        this.selectorType = selectorType;
        this.defaultValue = defaultValue;
    }

}
