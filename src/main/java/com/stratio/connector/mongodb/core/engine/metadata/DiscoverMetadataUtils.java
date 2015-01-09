package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.stratio.connector.commons.metadata.IndexMetadataBuilder;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

public class DiscoverMetadataUtils {

    private DiscoverMetadataUtils() {

    }

    public static List<String> discoverField(DBCollection collection) {
        // TODO implement with mapReduceCommand
        // MapReduceCommand mapReduceCommand = new MapReduceCommand(inputCollection, map, reduce, outputCollection,
        // type, query)

        DBObject discoverFieldCommand = new BasicDBObject("mapreduce", collection.getName());
        discoverFieldCommand.put("map", "function() { for (var field in this) { emit(field, null); }}");
        discoverFieldCommand.put("reduce", "function(field, stuff) { return null; }");
        discoverFieldCommand.put("out", new BasicDBObject("inline", 1));
        CommandResult command = collection.getDB().command(discoverFieldCommand);
        BasicDBList results = (BasicDBList) command.get("results");
        Set<String> fields = new HashSet<>();
        if (results != null) {
            for (Object object : results) {
                DBObject bson = (DBObject) object;
                fields.add((String) bson.get("_id"));
            }
        }
        return new ArrayList<String>(fields);
    }

    public static List<IndexMetadata> discoverIndexes(DBCollection collection) {
        // TODO add TextIndex, Geospatial,etc...
        // TODO supported only simple, compound and hashed index
        // TODO remove _id
        // TODO return options?? e.g sparse, unique??
        // TODO custom (asc and desc)
        List<DBObject> indexInfo = collection.getIndexInfo();
        String db = collection.getDB().getName();
        String collName = collection.getName();

        List<IndexMetadata> indexMetadataList = new ArrayList<>(indexInfo.size());
        for (DBObject dbObject : indexInfo) {
            BasicDBObject key = (BasicDBObject) dbObject.get("key");

            IndexMetadataBuilder indexMetadataBuilder = new IndexMetadataBuilder(db, collName,
                            (String) dbObject.get("name"), getIndexType(key));
            for (String field : key.keySet()) {
                indexMetadataBuilder.addColumn(field, null);
            }
            indexMetadataList.add(indexMetadataBuilder.build());
        }
        return indexMetadataList;
    }

    private static IndexType getIndexType(BasicDBObject key) {
        boolean isDefault = true;
        IndexType indexType;

        Iterator<Object> iterator = key.values().iterator();

        while (iterator.hasNext() && isDefault == true) {
            isDefault = iterator.next().toString().equals("1");
        }

        if (isDefault) {
            indexType = IndexType.DEFAULT;
        } else {
            indexType = IndexType.CUSTOM;
        }
        return indexType;
    }
}
