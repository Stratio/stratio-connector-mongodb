package com.stratio.connector.meta;

import java.io.Serializable;
import  com.stratio.connector.meta.exception.UnsupportedOperationException;


import java.io.Serializable;

/**
 * Created by jmgomez on 10/07/14.
 */
public interface IMetadataProvider extends Serializable {

    public void createCatalog(String catalog) throws UnsupportedOperationException;

    public void createTable(String catalog, String table) throws UnsupportedOperationException;

    public void dropCatalog(String catalog) throws UnsupportedOperationException;

    public void dropTable(String catalog, String table) throws UnsupportedOperationException;

    public void createIndex(String catalog, String table, String... field) throws UnsupportedOperationException;
    
    public void dropIndex(String catalog, String table, String... field) throws UnsupportedOperationException;
    
    public void dropIndexes(String catalog, String table) throws UnsupportedOperationException;
}
