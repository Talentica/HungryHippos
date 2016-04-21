package com.talentica.hungryHippos.tester.web.job.data;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.talentica.hungryHippos.tester.web.job.output.data.JobOutput;

@Entity
public class Job implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_id")
	private Integer jobId;

	@Column(name = "job_uuid")
	private String uuid;

	@Enumerated(EnumType.STRING)
	@Column(name = "status")
	private STATUS status;

	@Column(name = "date_time_submitted")
	private Date dateTimeSubmitted;

	@Column(name = "date_time_started")
	private Date dateTimeStarted;

	@Column(name = "date_time_finished")
	private Date dateTimeFinished;

	@Column(name = "user_id")
	private Integer userId;

	@OneToOne(fetch = FetchType.EAGER, mappedBy = "job")
	@JoinColumn(name = "job_id", insertable = false, updatable = false)
	private JobInput jobInput;

	@OneToOne(fetch = FetchType.EAGER, mappedBy = "job")
	@JoinColumn(name = "job_id", insertable = false, updatable = false)
	private JobOutput jobOutput;

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