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

import com.stratio.crossdata.common.statements.structures.selectors.SelectorType;

/**
 * @author darroyo Set of options for the mongo connector. A default value is provided.
 */

public enum IndexOptions {

    INDEX_TYPE("index_type", SelectorType.STRING), COMPOUND_FIELDS("compound_fields", SelectorType.STRING), SPARSE(
            "sparse", SelectorType.BOOLEAN), UNIQUE("unique", SelectorType.BOOLEAN);

    private final String optionName;
    private final SelectorType selectorType;

    public String getOptionName() {
        return optionName;
    }

    public SelectorType getSelectorType() {
        return selectorType;
    }

    IndexOptions(String optionName, SelectorType selectorType) {
        this.optionName = optionName;
        this.selectorType = selectorType;
    }

}
