package com.talentica.hungryHippos.tester.web.job.status.data;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.talentica.hungryHippos.tester.web.job.status.ProcessInstance;

@Repository
public interface ProcessInstanceRepository extends CrudRepository<ProcessInstance, Integer> {

	List<ProcessInstance> findByJobId(Integer jobId);

}