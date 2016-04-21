package com.talentica.hungryHippos.tester.api.job;

import java.util.Date;

import org.joda.time.Interval;

public class Job {

	private Integer jobId;

	private String uuid;

	private STATUS status;

	private Date dateTimeSubmitted;

	private Date dateTimeStarted;

	private Date dateTimeFinished;

	private Integer userId;

	private JobInput jobInput;

	private JobOutput jobOutput;

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
		if (getDateTimeFinished() != null && getDateTimeStarted() != null) {
			Interval executionInterval = new Interval(getDateTimeStarted().getTime(), getDateTimeFinished().getTime());
			duration = executionInterval.toDuration();
		}
		return duration;
	}

	public Integer getJobId() {
		return jobId;
	}

	public void setJobId(Integer jobId) {
		this.jobId = jobId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

	public Date getDateTimeSubmitted() {
		return dateTimeSubmitted;
	}

	public void setDateTimeSubmitted(Date dateTimeSubmitted) {
		this.dateTimeSubmitted = dateTimeSubmitted;
	}

	public Date getDateTimeStarted() {
		return dateTimeStarted;
	}

	public void setDateTimeStarted(Date dateTimeStarted) {
		this.dateTimeStarted = dateTimeStarted;
	}

	public Date getDateTimeFinished() {
		return dateTimeFinished;
	}

	public void setDateTimeFinished(Date dateTimeFinished) {
		this.dateTimeFinished = dateTimeFinished;
	}

	public JobInput getJobInput() {
		return jobInput;
	}

	public void setJobInput(JobInput jobInput) {
		this.jobInput = jobInput;
	}

	public JobOutput getJobOutput() {
		return jobOutput;
	}

	public void setJobOutput(JobOutput jobOutput) {
		this.jobOutput = jobOutput;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public void setExecutionTimeInSeconds(Long executionTimeInSeconds) {
		this.executionTimeInSeconds = executionTimeInSeconds;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof Job && jobId != null) {
			Job other = (Job) obj;
			return jobId.equals(other.jobId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (jobId != null) {
			return jobId.hashCode();
		}
		return 0;
	}

}