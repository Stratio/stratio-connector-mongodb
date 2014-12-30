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

package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.crossdata.common.exceptions.ExecutionException;

/**
 * The Class MongoQueryException.
 */
public class MongoExecutionException extends ExecutionException {

    /** The Constant serialVersionUID. */
    static final long serialVersionUID = 2782686873531397631L;

    /**
     * Instantiates a new mongo query exception.
     *
     * @param msg
     *            the msg
     * @param cause
     *            the cause
     */
    public MongoExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Instantiates a new mongo query exception.
     *
     * @param msg
     *            the msg
     */
    public MongoExecutionException(String msg) {
        super(msg);
    }
}