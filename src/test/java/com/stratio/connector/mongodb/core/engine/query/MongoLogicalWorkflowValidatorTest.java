package com.stratio.connector.mongodb.core.engine.query;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.mongodb.DBObject;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.connector.mongodb.core.exceptions.MongoValidationException;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderDirection;

@RunWith(PowerMockRunner.class)
public class MongoLogicalWorkflowValidatorTest {
    public static final String COLUMN_1 = "column1";
    public static final String COLUMN_2 = "column2";
    public static final String COLUMN_3 = "column3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    private static final ClusterName CLUSTER_NAME = new ClusterName("clustername");
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    /**
     * readLogicalWorkflow() preparation test. MongoLogicalWorkflowValidator
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void logicalWorkflowExecutorPreparationTest() throws Exception {
        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addEqualFilter(COLUMN_1, 5, false, false);
        logWorkFlowCreator.addColumnName(COLUMN_1);
        logWorkFlowCreator.addColumnName(COLUMN_2);
        logWorkFlowCreator.addGroupBy(COLUMN_MONEY, COLUMN_3);
        logWorkFlowCreator.addLimit(5);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.build();

        ProjectParsed logWorkflowData = new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0),
                        new MongoLogicalWorkflowValidator());

        assertEquals("The project should contain the catalog" + CATALOG, CATALOG, logWorkflowData.getProject()
                        .getCatalogName());
        assertEquals("The number of filters should be 1", 1, logWorkflowData.getFilter().size());
        assertEquals("The filter should have a equal relation", Operator.EQ,
                        (logWorkflowData.getFilter().iterator().next()).getRelation().getOperator());
        assertEquals("The limit value should be 5", 5, logWorkflowData.getLimit().getLimit());
        assertEquals("The select should have 2 columns", 2, logWorkflowData.getSelect().getColumnMap().size());
        assertEquals("The groupBy should have 2 ids", 2, logWorkflowData.getGroupBy().getIds().size());

    }

    /**
     * readLogicalWorkflow() validation test. MongoLogicalWorkflowValidator
     *
     * @throws Exception
     */
    @Test(expected = MongoValidationException.class)
    public void logicalWorkflowExecutorNoSelectTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.build();

        // removing the select. Adding a limit instead of the select
        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_LIMIT);
        for (LogicalStep logElement : logicalWorkflow.getLastStep().getPreviousSteps()) {
            logElement.setNextStep(new Limit(operations, 5));
        }

        new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0), new MongoLogicalWorkflowValidator());

    }

    @Test(expected = UnsupportedException.class)
    public void fulltextFilterTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addMatchFilter(COLUMN_1, "any");
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.build();

        new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0), new MongoLogicalWorkflowValidator());
    }

    /**
     * buildQuery() test
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void logicalWorkflowExecutorPrepareQueryTest() throws Exception {

        // Verify a ordinary query
        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        logWorkFlowCreator.addEqualFilter(COLUMN_1, 5, false, false);
        logWorkFlowCreator.addGreaterFilter(COLUMN_2, 1, false);
        logWorkFlowCreator.addColumnName(COLUMN_1);
        logWorkFlowCreator.addColumnName(COLUMN_2);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.build();

        ProjectParsed logWorkflowData = new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0),
                        new MongoLogicalWorkflowValidator());
        LogicalWorkflowExecutor lwExecutor = LogicalWorkflowExecutorFactory.getLogicalWorkflowExecutor(logWorkflowData);

        List<DBObject> query = (List<DBObject>) Whitebox.getInternalState(lwExecutor, "query");

        assertEquals("There should be only 1 query", 1, query.size());

        // Verify an aggregation query
        logWorkFlowCreator.addLimit(5);
        logWorkFlowCreator.addOrderByClause(COLUMN_3, OrderDirection.ASC);
        logWorkFlowCreator.addGroupBy(COLUMN_AGE);

        logicalWorkflow = logWorkFlowCreator.build();

        logWorkflowData = new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0),
                        new MongoLogicalWorkflowValidator());
        lwExecutor = LogicalWorkflowExecutorFactory.getLogicalWorkflowExecutor(logWorkflowData);

        query = (List<DBObject>) Whitebox.getInternalState(lwExecutor, "query");

        assertEquals("The aggregation framework should include 5 stages", 5, query.size());

    }

    // @Test
    // public void executeBasicQueryTest() throws NoSuchMethodException, SecurityException, UnsupportedException,
    // ExecutionException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    //
    // LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
    // logWorkFlowCreator.addEqualFilter(COLUMN_1, 5, false, false);
    // logWorkFlowCreator.addGreaterFilter(COLUMN_2, 1, false);
    // logWorkFlowCreator.addColumnName(COLUMN_1);
    // logWorkFlowCreator.addColumnName(COLUMN_2);
    //
    // LogicalWorkflow logicalWorkflow = logWorkFlowCreator.getLogicalWorkflow();
    //
    // LogicalWorkflowExecutor lwExecutor = new LogicalWorkflowExecutor(logicalWorkflow.getInitialSteps().get(0));
    //
    // DBCollection collection = Mockito.mock(DBCollection.class);
    // Method method = lwExecutor.getClass().getDeclaredMethod("executeBasicQuery", DBCollection.class);
    // method.setAccessible(true);
    //
    // ResultSet resultSet = (ResultSet) method.invoke(lwExecutor, (Collection<Filter>) null);
    //
    // }
}
