/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.mongodb.core.configuration;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.LOCAL_THREADSHOLD;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.CONNECTION_TIMEOUT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.MAX_CONNECTIONS_PER_HOST;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.MAX_IDLE_TIME;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.PORT;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.READ_PREFERENCE;
import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.WRITE_CONCERN;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.stratio.connector.commons.util.PropertyValueRecovered;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the configuration for Mongo connector.
 */

public class MongoClientConfiguration {


    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The configuration Manager.
     */
    private ConfigurationManager configurationManager;

    /** The connector cluster configuration. */
    private ConnectorClusterConfig configuration;

    /**
     * Instantiates a new mongo client configuration.
     *
     * @param connectorClusterConfig
     *            the connector cluster config
     */
    public MongoClientConfiguration(ConnectorClusterConfig connectorClusterConfig) {
        this.configuration = connectorClusterConfig;
        configurationManager = new ConfigurationManager(configuration);
    }

    /**
     * Retrieves the mongo client options. if any option is not specified a default value is set.
     *
     * @return the options for the java MongoDB driver
     * @throws MongoValidationException
     *             the mongo validation exception
     */
    public MongoClientOptions getMongoClientOptions() throws MongoValidationException {

        int localThreashold = getIntegerSetting(LOCAL_THREADSHOLD);
        int maxConnectionsPerHost = getIntegerSetting(MAX_CONNECTIONS_PER_HOST);
        int connectTimeout = getIntegerSetting(CONNECTION_TIMEOUT);
        int maxConnectionIdleTime = getIntegerSetting(MAX_IDLE_TIME);

        ReadPreference readPreference = getReadPreference();

        WriteConcern writeConcern = getWriteConcern();

        MongoClientOptions clientOptions = new MongoClientOptions.Builder()
                        .localThreshold(localThreashold)
                        .connectionsPerHost(maxConnectionsPerHost).connectTimeout(connectTimeout)
                        .maxConnectionIdleTime(maxConnectionIdleTime).readPreference(readPreference)
                        .writeConcern(writeConcern).build();

        return clientOptions;
    }

    /**
     * Gets the seeds.
     *
     * @return the seeds
     * @throws ConnectionException
     * @throws ConnectionException
     *             if the list of server address cannot be retrieved
     */
    public List<ServerAddress> getSeeds() throws ConnectionException {

        ArrayList<ServerAddress> seeds = new ArrayList<ServerAddress>();
        String[] hosts = configurationManager.recoverConfigurationValue(HOST);
        String[] ports = configurationManager.recoverConfigurationValue(PORT);

        // TODO
        if (hosts.length < 1 || (hosts.length != ports.length)) {
            throw new ConnectionException("invalid address");
        } else {
            for (int i = 0; i < hosts.length; i++) {

                try {
                    seeds.add(new ServerAddress(hosts[i], Integer.parseInt(ports[i])));
                } catch (NumberFormatException e) {
                    throw new ConnectionException("wrong port format " + ports[i], e);
                } catch (UnknownHostException e) {
                    throw new ConnectionException("connection failed with" + hosts[i], e);
                }

            }

        }

        return seeds;

    }

    /**
     * Recovered the option value or the default value if the property is not set.
     * @param configurationOption the configuration option.
     * @return the configuration option value.
     * @throws ConnectionException if the property is not set.
     */
    private String[] recoverConfigurationOption(ConfigurationOptions configurationOption) throws ConnectionException {
        String[] optionValue = {};
        try {
            Map<String, String> config = configuration.getClusterOptions();
            if (config != null) {
                String strPorts = config.get(configurationOption.getOptionName());
                if (strPorts != null) {
                    optionValue = PropertyValueRecovered.recoveredValueASArray(String.class, strPorts);
                } else {
                    optionValue = configurationOption.getDefaultValue();
                }
            } else {
                optionValue = configurationOption.getDefaultValue();
            }
        }catch(ExecutionException e){
            String msg = String.format("Error recovering [%s] option ",configurationOption.getOptionName());
            logger.error(msg);
            throw new ConnectionException(msg,e);
        }
        return optionValue;
    }

    /**
     * Retrieve the option value.
     *
     * @param option
     *            the option
     * @return the value specified. If not exist, the default value is returned
     * @throws MongoValidationException
     *             if the value can't be parsed to int
     */
    private int getIntegerSetting(ConfigurationOptions option) throws MongoValidationException {
        Map<String, String> config = configuration.getClusterOptions();
        int value;
        if (config != null && config.containsKey(option.getOptionName())) {
            try {
                value = Integer.parseInt(config.get(option.getOptionName()));
            } catch (NumberFormatException numberFormatException) {
                throw new MongoValidationException("The " + option.getOptionName() + " must be an integer",
                                numberFormatException);
            }
        } else {
            value = Integer.parseInt(option.getDefaultValue()[0]);
        }
        return value;

    }

    /**
     * Gets the read preference.
     *
     * @return the read preference specified. If not exist, the default value is returned
     * @throws MongoValidationException
     *             if the value cannot be parsed to ReadPreference
     */
    private ReadPreference getReadPreference() throws MongoValidationException {
        Map<String, String> config = configuration.getClusterOptions();
        ReadPreference readPreference;

        if (config != null && config.containsKey(READ_PREFERENCE.getOptionName())) {
            readPreference = settingToReadPreference(config.get(READ_PREFERENCE.getOptionName()));
        } else {
            readPreference = settingToReadPreference(READ_PREFERENCE.getDefaultValue()[0]);
        }
        return readPreference;

    }

    /**
     * Convert the Mongo connector string option to the appropriate Read Preference.
     *
     * @param readSetting
     *            the read preference string setting
     * @return the read preference.
     * @throws MongoValidationException
     *             if the value cannot be parsed to a ReadPreference
     */
    private ReadPreference settingToReadPreference(String readSetting) throws MongoValidationException {
        ReadPreference readPreference = null;
        switch (readSetting.trim().toLowerCase()) {
        case "primary":
            readPreference = ReadPreference.primary();
            break;
        case "primarypreferred":
            readPreference = ReadPreference.primaryPreferred();
            break;
        case "secondary":
            readPreference = ReadPreference.secondary();
            break;
        case "secondarypreferred":
            readPreference = ReadPreference.secondaryPreferred();
            break;
        case "nearest":
            readPreference = ReadPreference.nearest();
            break;
        default:
            throw new MongoValidationException("Read preference " + readSetting + " is not a legal value");
        }
        return readPreference;

    }

    /**
     * Gets the write concern.
     *
     * @return the write concern specified. If not exist, the default value is returned
     * @throws MongoValidationException
     *             if the value cannot be parsed to WriteConcern
     */
    private WriteConcern getWriteConcern() throws MongoValidationException {
        Map<String, String> config = configuration.getClusterOptions();
        WriteConcern writeConcern;

        if (config != null && config.containsKey(WRITE_CONCERN.getOptionName())) {
            writeConcern = settingToWritePreference(config.get(WRITE_CONCERN.getOptionName()));
        } else {
            writeConcern = settingToWritePreference(WRITE_CONCERN.getDefaultValue()[0]);
        }
        return writeConcern;

    }

    /**
     * Convert the mongo connector string option to the appropriate write concern.
     *
     * @param writeSetting
     *            the write concern string setting
     * @return the write concern
     * @throws MongoValidationException
     *             if the value cannot be parsed to a WriteConcern
     */
    private WriteConcern settingToWritePreference(String writeSetting) throws MongoValidationException {

        WriteConcern writeConcern = null;
        switch (writeSetting.trim().toLowerCase()) {
        case "acknowledged":
            writeConcern = WriteConcern.ACKNOWLEDGED;
            break;
        case "unacknowledged":
            writeConcern = WriteConcern.UNACKNOWLEDGED;
            break;
        case "replica_acknowledged":
            writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            break;
        case "journaled":
            writeConcern = WriteConcern.JOURNALED;
            break;
        default:
            throw new MongoValidationException("Write preference " + writeSetting + " is not a legal value");
        }
        return writeConcern;

    }

}
