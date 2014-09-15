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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;


public class FilterDBObjectBuilder extends DBObjectBuilder {

	private BasicDBObject filterQuery;
	private BasicDBObject filterOptions;
	
	
	public FilterDBObjectBuilder(boolean useAggregation){
		super(/*DBObjectType.FILTER,*/useAggregation);
		filterQuery = new BasicDBObject();
		//TODO filterQuery = QueryBuilder.start()...
			
	}

	public void add(Filter filter) {
		
		//add booleanType o logicalType
		

		Relation relation = filter.getRelation();
		
		if(filterQuery.containsField(getFieldName(relation.getLeftTerm()))){
			filterOptions = (BasicDBObject) filterQuery.get(getFieldName(relation.getLeftTerm()));	
		}else {
			filterOptions = new BasicDBObject();
		}
		
		
		
		switch(relation.getOperator()){
		
			case BETWEEN:
				new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW mewtodo handleBetweenFilter
				/*
				RelationBetween relBetween = (RelationBetween) relation;
				//check types: DateTerm, StringTerm, etc.. (única forma) de compatibilidad
				//múltiples between?? //check 2 terms
				
					filterOptions.append("$gte", relBetween.getTerms().get(0).getTermValue());
		//					Integer.valueOf(  relBetween.getTerms().get(0).getStringValue() ));
					filterOptions.append("$lte", relBetween.getTerms().get(1).getTermValue());
		//					Integer.valueOf(  relBetween.getTerms().get(1).getStringValue() ));	
					filterQuery.append(relation.getIdentifiers().get(0).getField(), filterOptions);
				
				*/
				
				break;
				
				
			case COMPARE:
				new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW mewtodo handleBetweenFilter
				/*
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
				
				*/
				break;
				
				
			case IN:
				new RuntimeException("A la espera de que se implemente por Meta"); //REVIEW mewtodo handleBetweenFilter
				/*SelectorType type = relation.getLeftTerm().getType();
				
				RelationIn relIn = (RelationIn) relation;
				//check integer?? 
				
				ArrayList inTerms = new ArrayList();
				for(Term<?> term : relIn.getTerms()){
					//comprobar que insertar...y que no (hacerlo igual)
					inTerms.add(term.getTermValue());
				}	
				filterOptions.append("$in",  inTerms);
				filterQuery.append(relation.getIdentifiers().get(0).getField(), filterOptions);
				*/
				break;


	
		case DISTINCT: case GET: case GT: case LET: case LT: case ASSIGN:
			handleRelationCompare(relation);
		
			default: //TODO throwException
				break;
			
		}
		
	}
	
	
	/**
	 * @param operator 
	 * 
	 */
	private void handleRelationCompare(Relation relation) {
		Operator operator = relation.getOperator();
		String lValue = null;
		//check integer?? también hay que hacer para between strings
		//relation.getIdentifiers().get(0).getField(); //si llega un equal se eliminan > < etc..
		switch(operator){
		
			case DISTINCT: lValue = "$ne"; break;
			case GET: lValue = "$gte"; break;
			case GT: lValue = "$gt"; break;
			case LET: lValue = "$lte";	break;
			case LT:  lValue = "$lt"; break;
			case ASSIGN: lValue = "$eq"; break;
		}
		
		if(lValue != null){
			
			   	Selector selector = relation.getRightTerm();
			   	
				switch(selector.getType()){
				
					case BOOLEAN: filterOptions.append(lValue, ((BooleanSelector) selector).getValue() ); break;
					//TODO floating point get valued missing
					//case FLOATING_POINT: filterOptions.append(lValue, ((FloatingPointSelector) selector). ); break;
					case INTEGER: filterOptions.append(lValue, ((IntegerSelector) selector).getValue() ); break;
					case STRING:filterOptions.append(lValue, ((StringSelector) selector).getValue() ); break;
					case COLUMN: filterOptions.append(lValue, ((ColumnSelector) selector).getName().getName()); break;
					case ASTERISK: case FUNCTION: default: throw new RuntimeException("Not implemented yet");
						//break;
				}		
				//TODO CHECK if numbers is supported
				filterQuery.append(getFieldName(relation.getLeftTerm()), filterOptions);
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
	

	private static String getFieldName(Selector selector){
        String field = null;
        if (selector instanceof ColumnSelector) {
            ColumnSelector columnSelector = (ColumnSelector) selector;
            field = columnSelector.getName().getName();
        }
        return field;
    }
	

	 
	 
}
