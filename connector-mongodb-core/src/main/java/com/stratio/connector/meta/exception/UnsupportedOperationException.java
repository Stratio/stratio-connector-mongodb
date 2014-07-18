package com.stratio.connector.meta.exception;

/**
 * Created by jmgomez on 9/07/14.
 */
public class UnsupportedOperationException extends Exception {
    /**
     * Constructor.
     *
     * @param t   the original exception.
     * @param msg the exception's message
     */
    public UnsupportedOperationException(String msg, Throwable t) {
        super(msg, t);
    }

    /**
     * Constructor.
     *
     * @param msg the exception's message
     */
    public UnsupportedOperationException(String msg) {
        super(msg);
    }

}

