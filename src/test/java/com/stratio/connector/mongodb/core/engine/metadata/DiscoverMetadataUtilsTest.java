package com.stratio.connector.mongodb.core.engine.metadata;

import java.net.UnknownHostException;

import org.junit.Test;

import com.mongodb.MongoClient;

public class DiscoverMetadataUtilsTest {

    @Test
    public void discoverFieldTest() throws UnknownHostException {
        MongoClient client = new MongoClient("10.200.0.58", 27100);
        DiscoverMetadataUtils.discoverField(client.getDB("test").getCollection("coll"));
    }

    @Test
    public void discoverIndexesTest() throws UnknownHostException {
        MongoClient client = new MongoClient("10.200.0.58", 27100);
        DiscoverMetadataUtils.discoverIndexes(client.getDB("test").getCollection("coll"));
    }
}
