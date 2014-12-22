package com.stratio.connector.mongodb.ftest.connection;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.mongodb.core.MongoConnector;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ConnectorException;

public class ConnectionFT {

    private String CLUSTER_NAME = "clusterName";
    protected String SERVER_IP = "NotExist";
    protected String SERVER_PORT = "15";
    private String readPreference = "primary";
    private String writeConcern = "acknowledged";

    @Test(expected = ConnectionException.class)
    public void connectionTest() throws ConnectorException {
        MongoConnector connector = new MongoConnector();
        connector.init(null);
        connector.connect(null, getClusterConfig());
        connector.close(getClusterName());
    }

    private ConnectorClusterConfig getClusterConfig() {
        Map<String, String> optionsNode = new HashMap<>();
        optionsNode.put(HOST.getOptionName(), SERVER_IP);
        optionsNode.put(PORT.getOptionName(), SERVER_PORT);
        optionsNode.put(READ_PREFERENCE.getOptionName(), readPreference);
        optionsNode.put(WRITE_CONCERN.getOptionName(), writeConcern);

        return new ConnectorClusterConfig(getClusterName(), null, optionsNode);
    }

    private ClusterName getClusterName() {
        return new ClusterName(CLUSTER_NAME);
    }
}
