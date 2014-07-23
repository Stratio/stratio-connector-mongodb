package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.meta.common.exceptions.ExecutionException;


public class MongoValidationException extends ExecutionException {

    public MongoValidationException(String msg) {
        super(msg);
    }
}