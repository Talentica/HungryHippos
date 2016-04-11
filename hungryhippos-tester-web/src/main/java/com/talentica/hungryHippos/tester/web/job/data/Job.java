package com.talentica.hungryHippos.tester.web.job.data;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Transient;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.talentica.hungryHippos.tester.web.job.output.data.JobOutput;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "jobId")
public class Job {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_id")
	private Integer jobId;

	@Getter
	@Setter
	@Column(name = "job_uuid")
	private String uuid;

	@Getter
	@Setter
	@Enumerated(EnumType.STRING)
	@Column(name = "status")
	private STATUS status;

	@Getter
	@Setter
	@Column(name = "date_time_submitted")
	private Date dateTimeSubmitted;

	@Getter
	@Setter
	@Column(name = "date_time_finished")
	private Date dateTimeFinished;

	@Getter
	@Setter
	@Column(name = "user_id")
	private Integer userId;

	@Getter
	@Setter
	@OneToOne
	@JoinColumn(name = "job_id")
	private JobInput jobInput;

	@Getter
	@Setter
	@OneToOne
	@JoinColumn(name = "job_id")
	private JobOutput jobOutput;

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
		if (getDateTimeFinished() != null && getDateTimeSubmitted() != null) {
			Interval executionInterval = new Interval(getDateTimeSubmitted().getTime(),
					getDateTimeFinished().getTime());
			duration = executionInterval.toDuration();
		}
		return duration;
	}

	public static Job createNewJob() {
		Job job = new Job();
		job.setDateTimeSubmitted(DateTime.now().toDate());
		job.setStatus(STATUS.NOT_STARTED);
		// TODO: Remove hard coding of user id later.
		job.setUserId(1);
		job.setUuid(UUID.randomUUID().toString().toUpperCase());
		return job;
	}

}