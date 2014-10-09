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
import com.stratio.meta.common.connector.ConnectorClusterConfig;

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
     */
    public MongoClientOptions getMongoClientOptions() {

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
            hosts = (String[]) ConnectorParser.hosts(config.get(HOST.getOptionName()));
            ports = (String[]) ConnectorParser.ports(config.get(PORT.getOptionName()));
        } else {
            hosts = HOST.getDefaultValue();
            ports = PORT.getDefaultValue();
        }

        // TODO
        if (hosts.length < 1 || (hosts.length != ports.length))
            throw new CreateNativeConnectionException("invalid address");
        else {
            for (int i = 0; i < hosts.length; i++) {

                try {
                    seeds.add(new ServerAddress(hosts[i], Integer.parseInt(ports[i])));
                } catch (NumberFormatException e) {
                    throw new CreateNativeConnectionException("wrong port format", e);
                } catch (UnknownHostException e) {
                    throw new CreateNativeConnectionException("connection failed with" + hosts[i], e);
                    // TODO check if
                }

            }

        }

        return seeds;

    }

    private int getIntegerSetting(ConfigurationOptions option) {
        Map<String, String> config = configuration.getOptions();
        int value;
        if (config != null && config.containsKey(option.getOptionName())) {
            value = Integer.parseInt(config.get(option.getOptionName()));
        } else {
            value = Integer.parseInt(option.getDefaultValue()[0]);
        }
        return value;

    }

    private ReadPreference getReadPreference() {
        Map<String, String> config = configuration.getOptions();
        ReadPreference readPreference;

        if (config != null && config.containsKey(READ_PREFERENCE.getOptionName())) {
            readPreference = settingToReadPreference(config.get(READ_PREFERENCE.getOptionName()));
        } else {
            readPreference = settingToReadPreference(READ_PREFERENCE.getDefaultValue()[0]);
        }
        return readPreference;

    }

    private ReadPreference settingToReadPreference(String readSetting) {
        ReadPreference readPreference = null;
        switch (readSetting.toLowerCase()) {
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
            throw new RuntimeException("read preference " + readSetting + " is not a legal value");
        }
        return readPreference;

    }

    private WriteConcern getWriteConcern() {
        Map<String, String> config = configuration.getOptions();
        WriteConcern writeConcern;

        if (config != null && config.containsKey(WRITE_CONCERN.getOptionName())) {
            writeConcern = settingToWritePreference(config.get(WRITE_CONCERN.getOptionName()));
        } else {
            writeConcern = settingToWritePreference(WRITE_CONCERN.getDefaultValue()[0]);
        }
        return writeConcern;

    }

    private WriteConcern settingToWritePreference(String writeSetting) {

        WriteConcern writeConcern = null;
        switch (writeSetting.toLowerCase()) {
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
            throw new RuntimeException("read preference " + writeSetting + " is not a legal value");
        }
        return writeConcern;

    }

}
