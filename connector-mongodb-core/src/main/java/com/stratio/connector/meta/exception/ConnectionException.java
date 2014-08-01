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

package com.stratio.connector.meta.exception;

/**
 * Created by jmgomez on 9/07/14.
 */
public class ConnectionException extends Exception {
    /**
     * Constructor.
     *
     * @param t   the original exception.
     * @param msg the exception's message
     */
    public ConnectionException(String msg, Throwable t) {
        super(msg, t);
    }

    /**
     * Constructor.
     *
     * @param msg the exception's message
     */
    public ConnectionException(String msg) {
        super(msg);
    }

}

