package com.talentica.hungryHippos.tester.web.user.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.api.ServiceError;
import com.talentica.hungryHippos.tester.web.user.data.Role;
import com.talentica.hungryHippos.tester.web.user.data.RoleRepository;
import com.talentica.hungryHippos.tester.web.user.data.User;
import com.talentica.hungryHippos.tester.web.user.data.UserRepository;

@Controller
@RequestMapping("/user")
public class UserAccountService {

	private UserRepository userRepository;

	@Autowired(required = false)
	private RoleRepository roleRepository;

	@RequestMapping(method = RequestMethod.POST)
	public @ResponseBody UserAccountServiceResponse create(
			@RequestBody(required = true) UserAccountServiceRequest request) {
		ServiceError error = request.validate();
		UserAccountServiceResponse response = new UserAccountServiceResponse();
		if (error != null) {
			response.setError(error);
			return response;
		}
		User user = request.getUser();
		String emailAddress = user.getEmailAddress();
		if (userRepository.findByEmailAddress(emailAddress) != null) {
			error = new ServiceError();
			error.setMessage("User with email address: " + emailAddress + " is already registered.");
			response.setError(error);
			return response;
		}
		Role userRole = roleRepository.findByRole("USER");
		user.getRoles().add(userRole);
		User savedUserDetail = userRepository.save(user);
		response.setUser(savedUserDetail);
		return response;
	}

	@RequestMapping(method = RequestMethod.GET)
	public @ResponseBody UserAccountServiceResponse get(@RequestParam(required = true) String emailAddress) {
		User user = userRepository.findByEmailAddress(emailAddress);
		UserAccountServiceResponse accountServiceResponse = new UserAccountServiceResponse();
		accountServiceResponse.setUser(user);
		return accountServiceResponse;
	}

	@Autowired(required = false)
	public void setUserRepository(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	public void setRoleRepository(RoleRepository roleRepository) {
		this.roleRepository = roleRepository;
	}

}