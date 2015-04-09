package com.stratio.connector.mongodb.core.engine.query;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.test.util.LogicalWorkFlowCreator;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;

@RunWith(PowerMockRunner.class)
public class LogicalWorkflowExecutorFactoryTest {

    public static final String COLUMN_1 = "column1";
    private static final ClusterName CLUSTER_NAME = new ClusterName("clustername");
    public static final String TABLE = "table_unit_test";
    public static final String CATALOG = "catalog_unit_test";

    /**
     * aggregationRequired() test
     *
     * @throws Exception
     */
    @Test
    public void logicalWorkflowExecutorFactoryTest() throws Exception {

        LogicalWorkFlowCreator logWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE, CLUSTER_NAME);
        LogicalWorkflow logicalWorkflow = logWorkFlowCreator.addColumnName(COLUMN_1).build();
        ProjectParsed logWorkflowData = new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0),
                        new MongoLogicalWorkflowValidator());
        LogicalWorkflowExecutor lwExecutor = LogicalWorkflowExecutorFactory.getLogicalWorkflowExecutor(logWorkflowData);
        assertTrue("The aggregation should not be required without groupBy statements",
                        lwExecutor instanceof BasicLogicalWorkflowExecutor);

        logWorkFlowCreator.addGroupBy(COLUMN_1);
        logicalWorkflow = logWorkFlowCreator.build();
        logWorkflowData = new ProjectParsed((Project) logicalWorkflow.getInitialSteps().get(0),
                        new MongoLogicalWorkflowValidator());
        lwExecutor = LogicalWorkflowExecutorFactory.getLogicalWorkflowExecutor(logWorkflowData);
        assertTrue("The aggregation should be required with groupBy statements",
                        lwExecutor instanceof AggregationLogicalWorkflowExecutor);

    }
}
