package com.talentica.hungryHippos.tester.web.job.data;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobRepository extends CrudRepository<Job, Integer> {

	Job findByUuidAndUserId(String uuid, Integer userId);

	Job findByUuid(String uuid);

	List<Job> findTop5ByUserIdOrderByDateTimeSubmittedDesc(Integer userId);

}
