package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.meta.common.exceptions.ExecutionException;

public class MongoQueryException extends ExecutionException {

    public MongoQueryException(String msg, Throwable cause) {
        super(msg);
    } 
}