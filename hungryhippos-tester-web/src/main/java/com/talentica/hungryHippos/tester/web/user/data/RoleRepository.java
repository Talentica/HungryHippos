package com.talentica.hungryHippos.tester.web.user.data;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RoleRepository extends CrudRepository<Role, Integer> {

	Role findByRole(String role);

}
