package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.meta.common.exceptions.ExecutionException;

public class MongoInsertException extends ExecutionException {

    public MongoInsertException(String msg, Throwable cause) {
        super(msg);
    } 
}