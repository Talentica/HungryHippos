package com.talentica.hungryHippos.tester.web.job.output.data;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.talentica.hungryHippos.tester.web.job.data.Job;

@Entity
public class JobOutput {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_output_id")
	private Integer jobOutputId;

	@OneToOne
	@JoinColumn(name = "job_id")
	@JsonIgnore
	private Job job;

	@Column(name = "data_location")
	private String dataLocation;

	@Column(name = "data_size_in_kbs")
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
