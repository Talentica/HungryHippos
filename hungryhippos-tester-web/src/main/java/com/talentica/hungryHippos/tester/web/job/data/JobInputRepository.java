package com.talentica.hungryHippos.tester.web.job.data;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobInputRepository extends CrudRepository<JobInput, Integer> {

}