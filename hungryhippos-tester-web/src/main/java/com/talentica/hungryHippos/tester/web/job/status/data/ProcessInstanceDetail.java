package com.talentica.hungryHippos.tester.web.job.status.data;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Transient;

import org.joda.time.Interval;

import com.talentica.hungryHippos.tester.web.job.data.STATUS;

@Entity
public class ProcessInstanceDetail {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_instance_detail_id")
	private Integer processInstanceDetailId;

	@Column(name = "process_instance_id")
	private Integer processInstanceId;

	@Column(name = "node_id")
	private Integer nodeId;

	@Column(name = "node_ip")
	private String nodeIp;

	@Enumerated(EnumType.STRING)
	@Column(name = "status")
	private STATUS status;

	@Column(name = "execution_start_time")
	private Date executionStartDateTime;

	@Column(name = "execution_end_time")
	private Date executionEndDateTime;

	@Column(name = "error_message")
	private String error;

	@Transient
	private Long executionTimeInSeconds;

	public void setExecutionTimeInSeconds() {
		org.joda.time.Duration duration = getExecutionDuration();
		if (duration != null) {
			executionTimeInSeconds = duration.getStandardSeconds();
		}
	}

	public Long getExecutionTimeInSeconds() {
		setExecutionTimeInSeconds();
		return executionTimeInSeconds;
	}

	private org.joda.time.Duration getExecutionDuration() {
		org.joda.time.Duration duration = null;
		if (executionStartDateTime != null && executionEndDateTime != null) {
			Interval executionInterval = new Interval(executionStartDateTime.getTime(), executionEndDateTime.getTime());
			duration = executionInterval.toDuration();
		}
		return duration;
	}

	public Integer getProcessInstanceDetailId() {
		return processInstanceDetailId;
	}

	public void setProcessInstanceDetailId(Integer processInstanceDetailId) {
		this.processInstanceDetailId = processInstanceDetailId;
	}

	public Integer getProcessInstanceId() {
		return processInstanceId;
	}

	public void setProcessInstanceId(Integer processInstanceId) {
		this.processInstanceId = processInstanceId;
	}

	public Integer getNodeId() {
		return nodeId;
	}

	public void setNodeId(Integer nodeId) {
		this.nodeId = nodeId;
	}

	public String getNodeIp() {
		return nodeIp;
	}

	public void setNodeIp(String nodeIp) {
		this.nodeIp = nodeIp;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

	public Date getExecutionStartDateTime() {
		return executionStartDateTime;
	}

	public void setExecutionStartDateTime(Date executionStartDateTime) {
		this.executionStartDateTime = executionStartDateTime;
		setExecutionTimeInSeconds();
	}

	public Date getExecutionEndDateTime() {
		return executionEndDateTime;
	}

	public void setExecutionEndDateTime(Date executionEndDateTime) {
		this.executionEndDateTime = executionEndDateTime;
		setExecutionTimeInSeconds();
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public void setExecutionTimeInSeconds(Long executionTimeInSeconds) {
		this.executionTimeInSeconds = executionTimeInSeconds;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof ProcessInstanceDetail && processInstanceDetailId != null) {
			ProcessInstanceDetail other = (ProcessInstanceDetail) obj;
			return processInstanceDetailId.equals(other.processInstanceDetailId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (processInstanceDetailId != null) {
			return processInstanceDetailId.hashCode();
		}
		return 0;
	}

}
