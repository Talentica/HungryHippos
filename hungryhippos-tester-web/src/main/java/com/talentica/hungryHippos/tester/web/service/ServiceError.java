package com.talentica.hungryHippos.tester.web.service;

import java.io.Serializable;

public class ServiceError implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String message;

	private String detail;

	public ServiceError() {

	}

	public ServiceError(String message, String detail) {
		this.message = message;
		this.detail = detail;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

}
