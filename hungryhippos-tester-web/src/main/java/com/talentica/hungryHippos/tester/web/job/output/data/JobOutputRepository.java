package com.talentica.hungryHippos.tester.web.job.output.data;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobOutputRepository extends CrudRepository<JobOutput, Integer> {

	JobOutput findByJobId(Integer jobId);

}