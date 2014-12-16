package com.stratio.connector.mongodb.core.engine.query.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;

public class ProjectBuilderTest {

    public static final String COLUMN_1 = "column1";
    public static final String ALIAS_COLUMN_1 = "alias1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    @Test
    public void projectDBObjectBuilderTest() throws MongoValidationException {

        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, ColumnType.VARCHAR));
        Select select = getSelect(columns);
        ProjectDBObjectBuilder projectBuilder = new ProjectDBObjectBuilder(false, select);
        DBObject expectedProject = new BasicDBObject(COLUMN_1, 1);
        expectedProject.put("_id", 0);
        DBObject internalProjectDBObject = (DBObject) Whitebox.getInternalState(projectBuilder, "projectQuery");

        Assert.assertEquals("The project is not the expected", expectedProject, internalProjectDBObject);

    }

    @Test
    public void buildTest() throws Exception {
        List<ConnectorField> columns;
        columns = Arrays.asList(new ConnectorField(COLUMN_1, ALIAS_COLUMN_1, ColumnType.VARCHAR));
        Select select = getSelect(columns);
        ProjectDBObjectBuilder projectBuilder = new ProjectDBObjectBuilder(false, select);
        DBObject fakeProject = new BasicDBObject(COLUMN_1, 1);

        // Default build
        Whitebox.setInternalState(projectBuilder, "projectQuery", fakeProject);
        Whitebox.setInternalState(projectBuilder, "useAggregation", false);

        DBObject projectCommand = projectBuilder.build();

        assertEquals("The value is not the expected", fakeProject, projectCommand);

        // Aggregation build
        Whitebox.setInternalState(projectBuilder, "useAggregation", true);

        projectCommand = projectBuilder.build();

        assertTrue("The project command should have a root $project", projectCommand.keySet().contains("$project"));
        assertEquals("The value is not the expected", fakeProject, projectCommand.get("$project"));

    }

    private Select getSelect(List<ConnectorField> fields) {
        Select select;
        Map<ColumnName, String> mapping = new LinkedHashMap<>();
        Map<String, ColumnType> types = new LinkedHashMap<>();
        Map<ColumnName, ColumnType> typeMapFormColumnName = new LinkedHashMap<>();
        for (ConnectorField connectorField : fields) {
            ColumnName columName = new ColumnName(CATALOG, TABLE, connectorField.name);
            mapping.put(columName, connectorField.alias);
            types.put(connectorField.alias, connectorField.columnType);
            typeMapFormColumnName.put(columName, connectorField.columnType);
        }

        select = new Select(Operations.PROJECT, mapping, types, typeMapFormColumnName);

        return select;

    }

    class ConnectorField {
        public String name;
        public String alias;
        public ColumnType columnType;

        public ConnectorField(String name, String alias, ColumnType columnType) {
            this.name = name;
            this.alias = alias;
            this.columnType = columnType;
        }

    }
}
