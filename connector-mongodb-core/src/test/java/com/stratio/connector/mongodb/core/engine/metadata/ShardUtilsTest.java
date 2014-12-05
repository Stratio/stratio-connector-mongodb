package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.stratio.connector.mongodb.core.configuration.ShardKeyType;
import com.stratio.connector.mongodb.core.configuration.TableOptions;
import com.stratio.connector.mongodb.testutils.TableMetadataBuilder;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MongoClient.class, DB.class, SelectorOptionsUtils.class })
public class ShardUtilsTest {

    private static final String DB_NAME = "catalog_name";
    private static final String COLLECTION_NAME = "tablename";
    private final String COLUMN_NAME = "colname";
    private final String COLUMN_NAME2 = "colname";

    @Mock
    MongoClient client;
    @Mock
    DB database;

    @Test
    public void isCollectionShardedTest() throws ExecutionException {
        Map<String, Selector> options;
        options = new HashMap<>();

        Assert.assertFalse("The result should be false by default", ShardUtils.isCollectionSharded(options));

        options.put(TableOptions.SHARDING_ENABLED.getOptionName(), new BooleanSelector(true));
        Assert.assertTrue("The result should be true", ShardUtils.isCollectionSharded(options));

        options.put(TableOptions.SHARDING_ENABLED.getOptionName(), new StringSelector("true"));
        Assert.assertTrue("The result should be true", ShardUtils.isCollectionSharded(options));

    }

    @Test
    public void shardCollectionDefaultTest() throws ExecutionException, UnsupportedException {
        //
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        Mockito.when(client.getDB("admin")).thenReturn(database);
        CommandResult commandRes = Mockito.mock(CommandResult.class);
        Mockito.when(commandRes.ok()).thenReturn(true);
        Mockito.when(database.command(Matchers.any(BasicDBObject.class))).thenReturn(commandRes);
        PowerMockito.mockStatic(SelectorOptionsUtils.class);
        Map<String, Selector> options = new HashMap<>();
        Mockito.when(SelectorOptionsUtils.processOptions(Matchers.anyMap())).thenReturn(options);

        ShardUtils.shardCollection(client, tableMetadataBuilder.build());

        Mockito.verify(database, Mockito.times(1)).command(new BasicDBObject("enableSharding", DB_NAME));
        BasicDBObject commandExpected = new BasicDBObject("shardCollection", DB_NAME + "." + COLLECTION_NAME);
        commandExpected.put("key", new BasicDBObject("_id", 1));
        Mockito.verify(database, Mockito.times(1)).command(commandExpected);

    }

    @Test
    public void shardCollectionHashedTest() throws ExecutionException, UnsupportedException {
        //
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        Mockito.when(client.getDB("admin")).thenReturn(database);
        CommandResult commandRes = Mockito.mock(CommandResult.class);
        Mockito.when(commandRes.ok()).thenReturn(true);
        Mockito.when(database.command(Matchers.any(BasicDBObject.class))).thenReturn(commandRes);
        PowerMockito.mockStatic(SelectorOptionsUtils.class);
        Map<String, Selector> options = new HashMap<>();
        options.put(TableOptions.SHARD_KEY_TYPE.getOptionName(), new StringSelector(ShardKeyType.HASHED.getKeyType()));
        options.put(TableOptions.SHARD_KEY_FIELDS.getOptionName(), new StringSelector(COLUMN_NAME));
        Mockito.when(SelectorOptionsUtils.processOptions(Matchers.anyMap())).thenReturn(options);

        ShardUtils.shardCollection(client, tableMetadataBuilder.build());

        Mockito.verify(database, Mockito.times(1)).command(new BasicDBObject("enableSharding", DB_NAME));
        BasicDBObject commandExpected = new BasicDBObject("shardCollection", DB_NAME + "." + COLLECTION_NAME);
        commandExpected.put("key", new BasicDBObject(COLUMN_NAME, "hashed"));
        Mockito.verify(database, Mockito.times(1)).command(commandExpected);

    }

    @Test
    public void shardCollectionMultiFieldsTest() throws ExecutionException, UnsupportedException {
        //
        TableMetadataBuilder tableMetadataBuilder = new TableMetadataBuilder(DB_NAME, COLLECTION_NAME);
        Mockito.when(client.getDB("admin")).thenReturn(database);
        CommandResult commandRes = Mockito.mock(CommandResult.class);
        Mockito.when(commandRes.ok()).thenReturn(true);
        Mockito.when(database.command(Matchers.any(BasicDBObject.class))).thenReturn(commandRes);
        PowerMockito.mockStatic(SelectorOptionsUtils.class);
        Map<String, Selector> options = new HashMap<>();
        options.put(TableOptions.SHARD_KEY_TYPE.getOptionName(), new StringSelector(ShardKeyType.ASC.getKeyType()));
        options.put(TableOptions.SHARD_KEY_FIELDS.getOptionName(), new StringSelector(COLUMN_NAME + "," + COLUMN_NAME2));
        Mockito.when(SelectorOptionsUtils.processOptions(Matchers.anyMap())).thenReturn(options);

        ShardUtils.shardCollection(client, tableMetadataBuilder.build());

        Mockito.verify(database, Mockito.times(1)).command(new BasicDBObject("enableSharding", DB_NAME));
        BasicDBObject keys = new BasicDBObject(COLUMN_NAME, 1);
        keys.append(COLUMN_NAME2, 1);
        BasicDBObject commandExpected = new BasicDBObject("shardCollection", DB_NAME + "." + COLLECTION_NAME);
        commandExpected.put("key", keys);
        Mockito.verify(database, Mockito.times(1)).command(commandExpected);

    }
}
