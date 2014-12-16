package com.stratio.connector.mongodb.core.engine.metadata;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.BasicDBObject;

public class AlterOptionsUtilsTest {

    @Test
    public void renameColumnTest() {
        BasicDBObject expected = new BasicDBObject("$rename", new BasicDBObject("old", "new"));
        Assert.assertEquals("The command is not the expected", expected,
                        AlterOptionsUtils.buildRenameColumnDBObject("old", "new"));
    }

    @Test
    public void dropColumnTest() {
        BasicDBObject expected = new BasicDBObject("$unset", new BasicDBObject("column", ""));
        Assert.assertEquals("The command is not the expected", expected,
                        AlterOptionsUtils.buildDropColumnDBObject("column"));
    }

}
