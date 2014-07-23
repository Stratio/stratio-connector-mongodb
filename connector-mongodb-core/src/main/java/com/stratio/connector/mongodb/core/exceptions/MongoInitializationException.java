package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.meta.common.exceptions.InitializationException;

public class MongoInitializationException extends InitializationException {


    public MongoInitializationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}