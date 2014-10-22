/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.mongodb.core.configuration;

import static com.stratio.connector.mongodb.core.configuration.ConfigurationOptions.ACCEPTABLE_LATENCY;
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
import com.stratio.connector.commons.connection.exceptions.CreateNativeConnectionException;
import com.stratio.connector.commons.util.ConnectorParser;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;

/**
 * The configuration for Mongo connector.
 */

public class MongoClientConfiguration {

    private ConnectorClusterConfig configuration;

    /**
     * @param connectorClusterConfig
     */
    public MongoClientConfiguration(ConnectorClusterConfig connectorClusterConfig) {
        this.configuration = connectorClusterConfig;
    }

    /**
     * @return the options for the java MongoDB driver
     * @throws MongoValidationException
     */
    public MongoClientOptions getMongoClientOptions() throws MongoValidationException {

        int acceptableLatencyDifference = getIntegerSetting(ACCEPTABLE_LATENCY);
        int maxConnectionsPerHost = getIntegerSetting(MAX_CONNECTIONS_PER_HOST);
        int connectTimeout = getIntegerSetting(CONNECTION_TIMEOUT);
        int maxConnectionIdleTime = getIntegerSetting(MAX_IDLE_TIME);

        ReadPreference readPreference = getReadPreference();

        WriteConcern writeConcern = getWriteConcern();

        MongoClientOptions clientOptions = new MongoClientOptions.Builder()
                        .acceptableLatencyDifference(acceptableLatencyDifference)
                        .connectionsPerHost(maxConnectionsPerHost).connectTimeout(connectTimeout)
                        .maxConnectionIdleTime(maxConnectionIdleTime).readPreference(readPreference)
                        .writeConcern(writeConcern).build();

        return clientOptions;
    }

    /**
     * @return the seeds
     */
    public List<ServerAddress> getSeeds() throws CreateNativeConnectionException {

        ArrayList<ServerAddress> seeds = new ArrayList<ServerAddress>();

        Map<String, String> config = configuration.getOptions();
        String[] hosts;
        String[] ports;
        if (config != null) {

            String strHosts = config.get(HOST.getOptionName());
            if (strHosts != null) {
                hosts = (String[]) ConnectorParser.hosts(strHosts);
            } else {
                hosts = HOST.getDefaultValue();
            }

            String strPorts = config.get(PORT.getOptionName());
            if (strPorts != null) {
                ports = (String[]) ConnectorParser.ports(strPorts);
            } else {
                ports = PORT.getDefaultValue();
            }

        } else {
            hosts = HOST.getDefaultValue();
            ports = PORT.getDefaultValue();
        }

        // TODO
        if (hosts.length < 1 || (hosts.length != ports.length)) {
            throw new CreateNativeConnectionException("invalid address");
        } else {
            for (int i = 0; i < hosts.length; i++) {

                try {
                    seeds.add(new ServerAddress(hosts[i], Integer.parseInt(ports[i])));
                } catch (NumberFormatException e) {
                    throw new CreateNativeConnectionException("wrong port format " + ports[i], e);
                } catch (UnknownHostException e) {
                    throw new CreateNativeConnectionException("connection failed with" + hosts[i], e);
                    // TODO check if
                }

            }

        }

        return seeds;

    }

    private int getIntegerSetting(ConfigurationOptions option) throws MongoValidationException {
        Map<String, String> config = configuration.getOptions();
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

    private ReadPreference getReadPreference() throws MongoValidationException {
        Map<String, String> config = configuration.getOptions();
        ReadPreference readPreference;

        if (config != null && config.containsKey(READ_PREFERENCE.getOptionName())) {
            readPreference = settingToReadPreference(config.get(READ_PREFERENCE.getOptionName()));
        } else {
            readPreference = settingToReadPreference(READ_PREFERENCE.getDefaultValue()[0]);
        }
        return readPreference;

    }

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
            throw new MongoValidationException("read preference " + readSetting + " is not a legal value");
        }
        return readPreference;

    }

    private WriteConcern getWriteConcern() throws MongoValidationException {
        Map<String, String> config = configuration.getOptions();
        WriteConcern writeConcern;

        if (config != null && config.containsKey(WRITE_CONCERN.getOptionName())) {
            writeConcern = settingToWritePreference(config.get(WRITE_CONCERN.getOptionName()));
        } else {
            writeConcern = settingToWritePreference(WRITE_CONCERN.getDefaultValue()[0]);
        }
        return writeConcern;

    }

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
            throw new MongoValidationException("write preference " + writeSetting + " is not a legal value");
        }
        return writeConcern;

    }

}
