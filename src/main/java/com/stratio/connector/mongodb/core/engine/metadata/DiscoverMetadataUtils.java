package com.stratio.connector.mongodb.core.engine.metadata;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.stratio.crossdata.common.metadata.IndexMetadata;

public class DiscoverMetadataUtils {

    private DiscoverMetadataUtils() {

    }

    public static List<String> discoverField(DBCollection collection) {

        // TODO implement with mapReduce
        DBObject discoverFieldCommand = new BasicDBObject("mapreduce", collection.getName());
        discoverFieldCommand.put("map", "function() { for (var field in this) { emit(field, null); }}");
        discoverFieldCommand.put("reduce", "function(field, stuff) { return null; }");
        discoverFieldCommand.put("out", "fields");
        CommandResult command = collection.getDB().command(discoverFieldCommand);
        String Idresponse = null;
        String result = command.getString(Idresponse);
        return new ArrayList<String>();
    }

    public static List<IndexMetadata> discoverIndexes(DBCollection collection) {
        // TODO Auto-generated method stub
        return null;
    }
}
