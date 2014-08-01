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