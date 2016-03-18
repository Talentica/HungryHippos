package com.talentica.hungryHippos.tester.web.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.domain.User;

@Controller
@RequestMapping("/user")
public class UserAccountService {

	@RequestMapping(method = RequestMethod.POST)
	public @ResponseBody User create(@RequestBody(required = false) User user) {

		return user;
	}

	@RequestMapping(method = RequestMethod.GET)
	public @ResponseBody User get(@RequestParam String emailAddress) {
		User user = new User();
		user.setFirstName("Nitin");
		user.setLastName("Kasat");
		user.setEmailAddress("nitin.kasat@talentica.com");
		user.setPassword("p@ssw0rd");
		return user;
    }


}