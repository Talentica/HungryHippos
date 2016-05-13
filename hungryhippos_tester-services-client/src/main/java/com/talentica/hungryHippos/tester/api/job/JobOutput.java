package com.talentica.hungryHippos.tester.api.job;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class JobOutput {

	private Integer jobOutputId;

	@JsonIgnore
	private Job job;

	private String dataLocation;

	private BigDecimal dataSize;

	public Integer getJobOutputId() {
		return jobOutputId;
	}

	public void setJobOutputId(Integer jobOutputId) {
		this.jobOutputId = jobOutputId;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public String getDataLocation() {
		return dataLocation;
	}

	public void setDataLocation(String dataLocation) {
		this.dataLocation = dataLocation;
	}

	public BigDecimal getDataSize() {
		return dataSize;
	}

	public void setDataSize(BigDecimal dataSize) {
		this.dataSize = dataSize;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof JobOutput && jobOutputId != null) {
			JobOutput other = (JobOutput) obj;
			return jobOutputId.equals(other.jobOutputId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (jobOutputId != null) {
			return jobOutputId.hashCode();
		}
		return 0;
	}

}
