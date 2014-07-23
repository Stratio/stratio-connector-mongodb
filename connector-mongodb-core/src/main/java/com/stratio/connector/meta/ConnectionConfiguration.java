package com.stratio.connector.meta;

/**
* Created by jmgomez on 21/07/14.
*/
public class ConnectionConfiguration {

    private ConnectionOption connectionOption;
    private Boolean mandatory;
    private Boolean multivalued;

    public ConnectionConfiguration(ConnectionOption connectionOption, Boolean mandatory, Boolean multivalued){
        this.connectionOption =connectionOption;
        this.mandatory = mandatory;
        this.multivalued = multivalued;
    }


    public Boolean isMandatory(){
        return mandatory;
    }

    public Boolean isMultivalued(){
        return multivalued;
    }


    public ConnectionOption getConnectionOption(){
        return connectionOption;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionConfiguration that = (ConnectionConfiguration) o;

        if (connectionOption != that.connectionOption) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return connectionOption != null ? connectionOption.hashCode() : 0;
    }

}