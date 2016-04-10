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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "processInstanceDetailId")
public class ProcessInstanceDetail {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_instance_detail_id")
	private Integer processInstanceDetailId;

	@Getter
	@Setter
	@Column(name = "process_instance_id")
	private Integer processInstanceId;

	@Getter
	@Setter
	@Column(name = "node_id")
	private Integer nodeId;

	@Getter
	@Setter
	@Column(name = "node_ip")
	private String nodeIp;

	@Getter
	@Setter
	@Enumerated(EnumType.STRING)
	@Column(name = "status")
	private STATUS status;

	@Getter
	@Setter
	@Column(name = "execution_start_time")
	private Date executionStartDateTime;

	@Getter
	@Setter
	@Column(name = "execution_end_time")
	private Date executionEndDateTime;

	@Getter
	@Setter
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

}
