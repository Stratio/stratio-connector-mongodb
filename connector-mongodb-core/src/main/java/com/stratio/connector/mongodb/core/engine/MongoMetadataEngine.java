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
package com.stratio.connector.mongodb.core.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;

/**
 * @author darroyo
 *
 */
public class MongoMetadataEngine implements IMetadataEngine{

	private static final String CATALOG = "metadata_storage";
	private static String COLLECTION = "default";
	
	
	private MongoStorageEngine storageEngine = null;
	private MongoQueryEngine queryEngine = null;
	
	

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#put(java.lang.String, java.lang.String)
	 */
	@Override
	public void put(String key, String metadata) {
		//TODO validate? key exist? =>key=type y id=1
		//get then insert?
		
		Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(key, new Cell(metadata));
        row.setCells(cells);
		storageEngine.insert(CATALOG, COLLECTION, row);
		
	}

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#get(java.lang.String)
	 */
	@Override
	public String get(String key) {
		//TODO getID, getPK

		List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = Arrays.asList(new ColumnMetadata(COLLECTION,key));
        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
		LogicalPlan logicalPlan = new LogicalPlan(stepList);
		
		
        QueryResult queryResult = null;
		try {
			queryResult = (QueryResult) queryEngine.execute(logicalPlan);
		} catch (ExecutionException e) {
			// TODO throws Auto-generated catch block
			e.printStackTrace();
		}
        
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        Row row = null;
        while(rowIterator.hasNext()){
        	if(row != null) {
        		row = null; break;//TODO throw new Exception
        	}else row = rowIterator.next();
        	
        }
        
        return (String) row.getCells().get(key).getValue();
        
	}

	/**
	 * @param elasticStorageEngine
	 */
	public void setStorageEngine(MongoStorageEngine mongoStorageEngine) {
		storageEngine = mongoStorageEngine;
		
	}
	
	/**
	 * @param elasticQueryEngine
	 */
	public void setQueryEngine(MongoQueryEngine mongoQueryEngine) {
		queryEngine  = mongoQueryEngine;
		
	}


}
