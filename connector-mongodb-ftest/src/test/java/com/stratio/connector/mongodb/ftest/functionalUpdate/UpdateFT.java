package com.stratio.connector.mongodb.ftest.functionalUpdate;

import static org.junit.Assert.assertEquals;

import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.ftest.functionalUpdate.GenericSimpleUpdateFT;
import com.stratio.connector.commons.ftest.helper.IConnectorHelper;
import com.stratio.connector.mongodb.ftest.helper.MongoConnectorHelper;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.InitializationException;

public class UpdateFT extends GenericSimpleUpdateFT{

    @Override
    protected IConnectorHelper getConnectorHelper() {
        MongoConnectorHelper mongoConnectorHelper = null;
        try {
            mongoConnectorHelper = new MongoConnectorHelper(getClusterName());
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (InitializationException e) {
            e.printStackTrace();
        } catch (CreateNativeConnectionException e) {
            e.printStackTrace();
        }
        return mongoConnectorHelper;
    }
    
   
}