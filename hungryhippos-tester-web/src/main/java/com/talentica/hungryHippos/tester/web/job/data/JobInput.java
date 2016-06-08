package com.talentica.hungryHippos.tester.web.job.data;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
public class JobInput implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_input_id")
	private Integer jobInputId;

	@OneToOne
	@JoinColumn(name = "job_id")
	@JsonIgnore
	private Job job;

	@Column(name = "data_location")
	private String dataLocation;

	@Column(name = "data_size_in_kbs")
	private BigDecimal dataSize;

	@Column(name = "data_type_configuration")
	private String dataTypeConfiguration;

	@Column(name = "sharding_dimensions")
	private String shardingDimensions;

	@Column(name = "job_matrix_class")
	private String jobMatrixClass;

	@Column(name = "data_parser_class")
	private String dataParserClass;

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

	public String getDataParserClass() {
		return dataParserClass;
	}

	public void setDataParserClass(String dataParserClass) {
		this.dataParserClass = dataParserClass;
	}

}
