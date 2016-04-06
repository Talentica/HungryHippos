package com.talentica.hungryHippos.tester.web.job.process.data;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessInstanceRepository extends CrudRepository<ProcessInstance, Integer> {

	List<ProcessInstance> findByJobId(Integer jobId);

}