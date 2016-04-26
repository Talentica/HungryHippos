package com.talentica.hungryHippos.tester.web.job.data;

import com.fasterxml.jackson.annotation.JsonValue;

public enum STATUS {

	NOT_STARTED("Not Started"), 
	IN_PROGRESS("In Progress"), 
	COMPLETED("Completed"), 
	FAILED("Failed");
	
	private String name;
	
	private STATUS(String name){
		this.name=name;
	}
	
	@JsonValue
	public String getName() {
		return name;
	}
	
	@Override
	public String toString(){
		return name;
	}
}
