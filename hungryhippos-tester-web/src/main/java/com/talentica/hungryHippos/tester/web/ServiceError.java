package com.talentica.hungryHippos.tester.web;

import lombok.Getter;
import lombok.Setter;

public class ServiceError {

	@Getter
	@Setter
	private String message;

	@Getter
	@Setter
	private String detail;

	public ServiceError() {

	}

	public ServiceError(String message, String detail) {
		this.message = message;
		this.detail = detail;
	}

}
