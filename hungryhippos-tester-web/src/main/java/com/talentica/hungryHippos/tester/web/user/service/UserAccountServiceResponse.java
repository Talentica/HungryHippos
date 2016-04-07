package com.talentica.hungryHippos.tester.web.user.service;

import com.talentica.hungryHippos.tester.web.service.ServiceError;
import com.talentica.hungryHippos.tester.web.user.data.User;

public class UserAccountServiceResponse {

	private User user;

	private ServiceError error;

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public ServiceError getError() {
		return error;
	}

	public void setError(ServiceError error) {
		this.error = error;
	}

}
