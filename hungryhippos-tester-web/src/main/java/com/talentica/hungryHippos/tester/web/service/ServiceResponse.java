package com.talentica.hungryHippos.tester.web.service;

import java.io.Serializable;

public class ServiceResponse implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ServiceError error;

	public ServiceError getError() {
		return error;
	}

	public void setError(ServiceError error) {
		this.error = error;
	}

}
