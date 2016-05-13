package com.talentica.hungryHippos.tester.web;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.talentica.hungryHippos.tester.web.user.data.User;
import com.talentica.hungryHippos.tester.web.user.data.UserRepository;

@Component
public final class UserCache {

	@Autowired(required = false)
	private UserRepository userRepository;

	private static final Map<String, User> USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE = new HashMap<>();

	public UserCache() {
	}

	private User getUserByEmailAddress(String emailAddress) {
		User user = USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE.get(emailAddress);
		if (user == null) {
			limitCacheSize();
			user = userRepository.findByEmailAddress(emailAddress);
			USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE.put(emailAddress, user);
		}
		return user;
	}

	private static void limitCacheSize() {
		if (USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE.size() > 1000) {
			synchronized (USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE) {
				USER_EMAIL_ADDRESS_TO_USER_ENTITIES_CACHE.clear();
			}
		}
	}

	public User getCurrentLoggedInUser() {
		String emailAddress = SecurityContextHolder.getContext().getAuthentication().getName();
		return getUserByEmailAddress(emailAddress);
	}

	public void setUserRepository(UserRepository userRepository) {
		this.userRepository = userRepository;
	}
}
