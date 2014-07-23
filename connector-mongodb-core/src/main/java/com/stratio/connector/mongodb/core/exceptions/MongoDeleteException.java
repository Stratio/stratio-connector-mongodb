package com.stratio.connector.mongodb.core.exceptions;

import com.stratio.meta.common.exceptions.ExecutionException;

public class MongoDeleteException extends ExecutionException {

    public MongoDeleteException(String msg, Throwable cause) {
        super(msg);
}
}