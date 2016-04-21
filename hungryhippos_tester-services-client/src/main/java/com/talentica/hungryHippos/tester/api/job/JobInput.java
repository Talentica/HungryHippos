package com.talentica.hungryHippos.tester.api.job;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class JobInput {

	private Integer jobInputId;

	@JsonIgnore
	private Job job;

	private String dataLocation;

	private BigDecimal dataSize;

	private String dataTypeConfiguration;

	private String shardingDimensions;

	private String jobMatrixClass;

	public Integer getJobInputId() {
		return jobInputId;
	}

	public void setJobInputId(Integer jobInputId) {
		this.jobInputId = jobInputId;
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

	public String getDataTypeConfiguration() {
		return dataTypeConfiguration;
	}

	public void setDataTypeConfiguration(String dataTypeConfiguration) {
		this.dataTypeConfiguration = dataTypeConfiguration;
	}

	public String getShardingDimensions() {
		return shardingDimensions;
	}

	public void setShardingDimensions(String shardingDimensions) {
		this.shardingDimensions = shardingDimensions;
	}

	public String getJobMatrixClass() {
		return jobMatrixClass;
	}

	public void setJobMatrixClass(String jobMatrixClass) {
		this.jobMatrixClass = jobMatrixClass;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof JobInput && jobInputId != null) {
			JobInput other = (JobInput) obj;
			return jobInputId.equals(other.jobInputId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (jobInputId != null) {
			return jobInputId.hashCode();
		}
		return 0;
	}

}
