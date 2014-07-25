package com.stratio.connector.mongodb.core.engine.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.stratio.connector.meta.GroupBy;



public class GroupByDBObjectBuilder extends DBObjectBuilder {

	private BasicDBObject groupQuery;

	
	
	public GroupByDBObjectBuilder(GroupBy... group){
		super(DBObjectType.GROUPBY,true);
		groupQuery = new BasicDBObject();
		
		String identifier;
		
		
		
//		  $group: {
//            _id: "$age",
//            users: { $push: { userid: "$user", score: "$score" } }
//          }
//		}
		//TODO cambiar con groupBy y agregación //comprobar que no haya
		//for ...

//		groupQuery.put("arrayTest", (new BasicDBObject("$push","$$ROOT")));//ya existe proyección previa
		
		
		//

			if(group.length == 1){
				identifier = group[0].getSelectorIdentifier().getField();
				groupQuery.put("_id", "$"+identifier);
			}else{
				BasicDBObject groupFields = new BasicDBObject();
				for (int i = 0; i< group.length; i++){
					identifier = group[i].getSelectorIdentifier().getField();
					groupFields.put(identifier, "$"+identifier); 
				}
				groupQuery.put("_id", groupFields);//si hay uno => solo incluir un campo
			}
		
		
		
	}

	public void add() { //incluir operadores de agregación
		
		
	}
	

	
	public DBObject build(){
		
		DBObject container;
		if(useAggregation){
			container = new BasicDBObject(); 
			container.put("$group", groupQuery);
		}else container = groupQuery;//no
		System.out.println(container);
		return container;
			
	}
}
