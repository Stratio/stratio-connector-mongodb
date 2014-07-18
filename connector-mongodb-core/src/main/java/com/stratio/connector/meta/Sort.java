package com.stratio.connector.meta;


import com.stratio.meta.common.logicalplan.LogicalStep;

public class Sort extends LogicalStep{

	
	public static final int ASC = 1;
	public static final int DESC = 2;
	
	private String fieldName;
	private int type;
	
	public Sort(String field,int type){
		fieldName = field;
		this.type = type;
	}

	public String getField() {
		return fieldName;
	}

	public int getType() {
		return type;
	}
	

}
