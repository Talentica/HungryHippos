package com.talentica.hungryHippos.tester.web.job.status.data;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;

@Entity
public class ProcessInstance {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_instance_id")
	private Integer processInstanceId;

	@OneToOne
	@JoinColumn(name = "process_id")
	private Process process;

	@Column(name = "job_id")
	private Integer jobId;

	@OneToMany(mappedBy = "processInstanceId", fetch = FetchType.EAGER)
	@PrimaryKeyJoinColumn
	private List<ProcessInstanceDetail> processInstanceDetails;

	public Integer getProcessInstanceId() {
		return processInstanceId;
	}

	public void setProcessInstanceId(Integer processInstanceId) {
		this.processInstanceId = processInstanceId;
	}

	public Process getProcess() {
		return process;
	}

	public void setProcess(Process process) {
		this.process = process;
	}

	public Integer getJobId() {
		return jobId;
	}

	public void setJobId(Integer jobId) {
		this.jobId = jobId;
	}

	public List<ProcessInstanceDetail> getProcessInstanceDetails() {
		return processInstanceDetails;
	}

	public void setProcessInstanceDetails(List<ProcessInstanceDetail> processInstanceDetails) {
		this.processInstanceDetails = processInstanceDetails;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof ProcessInstance && processInstanceId != null) {
			ProcessInstance other = (ProcessInstance) obj;
			return processInstanceId.equals(other.processInstanceId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (processInstanceId != null) {
			return processInstanceId.hashCode();
		}
		return 0;
	}

}
