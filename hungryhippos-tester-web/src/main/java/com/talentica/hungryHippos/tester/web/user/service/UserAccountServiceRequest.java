package com.talentica.hungryHippos.tester.web.user.service;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.talentica.hungryHippos.tester.api.ServiceError;
import com.talentica.hungryHippos.tester.web.user.data.User;

public class UserAccountServiceRequest {

	private User user;

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	@JsonIgnore
	public ServiceError validate() {
		ServiceError error = null;
		if (user == null) {
			error = new ServiceError();
			error.setMessage("No user information received in request.");
		} else if (StringUtils.isBlank(user.getEmailAddress()) || StringUtils.isBlank(user.getPassword())
				|| StringUtils.isBlank(user.getFirstName()) || StringUtils.isBlank(user.getLastName())) {
			error = new ServiceError();
			error.setMessage("First name, last name, email address and password cannot be blank.");
		} else if (user.getUserId() != null) {
			error = new ServiceError();
			error.setMessage("User is already set in the request submitted. Invalid request.");
		}
		return error;
	}

}
