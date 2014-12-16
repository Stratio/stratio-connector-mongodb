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
 * Set of index options.
 *
 */

public enum IndexOptions {

    /** The custom index type. */
    INDEX_TYPE("index_type", SelectorType.STRING),
    /** The compound fields. */
    COMPOUND_FIELDS("compound_fields", SelectorType.STRING),
    /** Wheter the index is sparse or not. */
    SPARSE("sparse", SelectorType.BOOLEAN),
    /** Wheter the index is unique or not. */
    UNIQUE("unique", SelectorType.BOOLEAN);

    /** The option name. */
    private final String optionName;

    /** The selector type. */
    private final SelectorType selectorType;

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
     * Instantiates a new index options.
     *
     * @param optionName
     *            the option name
     * @param selectorType
     *            the selector type
     */
    IndexOptions(String optionName, SelectorType selectorType) {
        this.optionName = optionName;
        this.selectorType = selectorType;
    }

}
