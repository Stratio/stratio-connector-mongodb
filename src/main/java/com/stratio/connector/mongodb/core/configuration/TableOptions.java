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

package com.stratio.connector.mongodb.core.configuration;

import com.stratio.crossdata.common.statements.structures.SelectorType;

/**
 * Set of table options for the mongo connector. A default value is provided.
 * 
 */

public enum TableOptions {

    /** The sharding enabled. */
    SHARDING_ENABLED("enable_sharding", SelectorType.BOOLEAN, false),
    /** The shard key type. */
    SHARD_KEY_TYPE("shard_key_type", SelectorType.STRING, ShardKeyType.ASC),
    /** The shard key fields. */
    SHARD_KEY_FIELDS("shard_key_fields", SelectorType.STRING, new String[] { "_id" });

    /** The option name. */
    private final String optionName;

    /** The selector type. */
    private final SelectorType selectorType;

    /** The default value. */
    private final Object defaultValue;

    /**
     * Gets the default value.
     *
     * @return the default value
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * Gets the option name.
     *
     * @return the option name
     */
    public String getOptionName() {
        return optionName;
    }

    /**
     * Gets the selector type.
     *
     * @return the selector type
     */
    public SelectorType getSelectorType() {
        return selectorType;
    }

    /**
     * Instantiates a new table options.
     *
     * @param optionName
     *            the option name
     * @param selectorType
     *            the selector type
     * @param defaultValue
     *            the default value
     */
    TableOptions(String optionName, SelectorType selectorType, Object defaultValue) {
        this.optionName = optionName;
        this.selectorType = selectorType;
        this.defaultValue = defaultValue;
    }

}
