package com.stratio.connector.meta;

import com.stratio.meta.common.logicalplan.LogicalStep;

public class Limit extends LogicalStep{

   /**
	*  Number of elements returned
	*/
	private int number;
	
	public Limit(int num){
		number = num;
	}
	
	public int getLimit(){
		return number;
	}
}
