package com.talentica.hungryHippos.tester.web.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.data.UserRepository;
import com.talentica.hungryHippos.tester.web.entities.User;

@Controller
@RequestMapping("/user")
public class UserAccountService {

	private UserRepository userRepository;

	@RequestMapping(method = RequestMethod.POST)
	public @ResponseBody User create(@RequestBody(required = true) User user) {
		user.validate();
		user = userRepository.save(user);
		return user;
	}

	@RequestMapping(method = RequestMethod.GET)
	public @ResponseBody User get(@RequestParam(required=true) String emailAddress) {
		return userRepository.findByEmailAddress(emailAddress);
	}

	@Autowired(required = false)
	public void setUserRepository(UserRepository userRepository) {
		this.userRepository = userRepository;
	}

}