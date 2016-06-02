/**
 * 
 */
package com.talentica.hungryHippos.client.domain;

import java.util.Arrays;

/**
 * @author pooshans
 *
 */
public class InvalidRowExeption extends Exception{
	private static final long serialVersionUID = 1L;
	private String message;
	private MutableCharArrayString row;
	private boolean[] columns;

	public InvalidRowExeption() {
		super();
	}

	public InvalidRowExeption(String message) {
		super(message);
		this.message = message;
	}
	
	public InvalidRowExeption(String message,boolean[] columns) {
		super(message);
		this.message = message;
		this.columns = columns;
	}
	
	public void setBadRow(MutableCharArrayString row){
		this.row = row;
	}
	
	public MutableCharArrayString getBadRow(){
		return this.row;
	}
	
	public boolean[] getColumns() {
		return columns;
	}

	public void setColumns(boolean[] columns) {
		this.columns = columns;
	}

	public InvalidRowExeption(Throwable cause) {
		super(cause);
	}

	@Override
	public String toString() {
		return message + " ["+this.row.toString() + "] having invalid columns : "+Arrays.toString(columns).toString();
	}

	@Override
	public String getMessage() {
		return message + " ["+this.row.toString() + "] having invalid columns : "+Arrays.toString(columns).toString();
	}
}
