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
package com.stratio.connector.mongodb.core.engine.utils;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.relationships.RelationBetween;
import com.stratio.meta.common.statements.structures.relationships.RelationCompare;
import com.stratio.meta.common.statements.structures.relationships.RelationIn;
import com.stratio.meta.common.statements.structures.relationships.RelationType;
import com.stratio.meta.common.statements.structures.terms.Term;

public class FilterDBObjectBuilder extends DBObjectBuilder {

	private BasicDBObject filterQuery;
	private BasicDBObject filterOptions;
	
	
	public FilterDBObjectBuilder(boolean useAggregation){
		super(DBObjectType.FILTER,useAggregation);
		filterQuery = new BasicDBObject();
			
	}

	public void add(Filter filter) {
		
		//add booleanType o logicalType
		
		RelationType relationType = filter.getType();
		Relation relation = filter.getRelation();
		
		if(filterQuery.containsField(relation.getIdentifiers().get(0).getField())){
			filterOptions = (BasicDBObject) filterQuery.get(relation.getIdentifiers().get(0).getField());	
		}else filterOptions = new BasicDBObject();
		
		switch(relationType){
		
		
			case BETWEEN:
				RelationBetween relBetween = (RelationBetween) relation;
				//check types: DateTerm, StringTerm, etc.. (única forma) de compatibilidad
				//múltiples between?? //check 2 terms
				
					filterOptions.append("$gte", relBetween.getTerms().get(0).getTermValue());
		//					Integer.valueOf(  relBetween.getTerms().get(0).getStringValue() ));
					filterOptions.append("$lte", relBetween.getTerms().get(1).getTermValue());
		//					Integer.valueOf(  relBetween.getTerms().get(1).getStringValue() ));	
					filterQuery.append(relation.getIdentifiers().get(0).getField(), filterOptions);
				
				break;
				
				
			case COMPARE:
				RelationCompare relCompare = (RelationCompare) relation;
				String lValue = null;
				//check integer?? también hay que hacer para between strings
				if(relCompare.getOperator().equals("=")) lValue = "$eq";
						//relation.getIdentifiers().get(0).getField(); //si llega un equal se eliminan > < etc..
				else if(relCompare.getOperator().equals(">")) lValue = "$gt";
				else if(relCompare.getOperator().equals(">=")) lValue = "$gte";
				else if(relCompare.getOperator().equals("<")) lValue = "$lt";
				else if(relCompare.getOperator().equals("<=")) lValue = "$lte";	
				else if(relCompare.getOperator().equals("<>") || relCompare.getOperator().equals("!=") ) lValue = "$ne";

				
				if(lValue != null){
					filterOptions.append(lValue, relCompare.getTerms().get(0).getTermValue() );
					filterQuery.append(relation.getIdentifiers().get(0).getField(), filterOptions);
				}
				
				
				break;
				
				
			case IN:
				
				RelationIn relIn = (RelationIn) relation;
				//check integer?? 
				
				ArrayList inTerms = new ArrayList();
				for(Term<?> term : relIn.getTerms()){
					//comprobar que insertar...y que no (hacerlo igual)
					inTerms.add(term.getTermValue());
				}	
				filterOptions.append("$in",  inTerms);
				filterQuery.append(relation.getIdentifiers().get(0).getField(), filterOptions);
				
				break;
			case TOKEN:
				break;
			default: //throwException
				break;
			
		}
		
	}
	
	public DBObject build(){
		DBObject container;
		if(useAggregation){
			container = new BasicDBObject(); 
			container.put("$match", filterQuery);
		}else container = filterQuery;
		
		return container;
			
	}
}
