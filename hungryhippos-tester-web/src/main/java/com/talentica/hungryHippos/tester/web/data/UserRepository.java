package com.talentica.hungryHippos.tester.web.data;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.talentica.hungryHippos.tester.web.entities.User;

@Repository
public interface UserRepository extends CrudRepository<User, Integer> {

	User findByEmailAddress(String emailAddress);

}
